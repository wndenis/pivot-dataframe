import numpy as np
import pandas as pd
import json
import io
from collections import OrderedDict


# временное решение для отключения print в файле
def opt_print(func):
    def wrapped(*args, **kwargs):
        if opt_print.do_print:
            return func(*args, **kwargs)

    return wrapped


opt_print.do_print = False  # True - включить print, False - выключить print
print = opt_print(print)

# упорядоченный словарь
# хранит порядок перехода от кириллицы к латинице
# хранит порядок столбцов в таблице слева направо
ru_en = OrderedDict(
    {
        "ПАО+ДЗО+ДИТ": "PAO_DZO_DIT",
        "ПАО+ДИТ": "PAO_DIT",
        "ПАО": "PAO",
        "ЦА": "CA",
        "ПЦП": "PCP",
        "ТБ": "TB",
        "ВСП": "VSP",
        "ДИТ": "DIT",
        "ДЗО": "DZO"
    }
)
# упорядоченный словарь для перехода от латиницы к кирилице
en_ru = OrderedDict({v: k for k, v in ru_en.items()})


def df_to_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Делает сводную таблицу для датафрейма с агрегацией ПАО и проч.
    :param df: датафрейм
    :return: сводный датафрейм
    """
    # создание сводной таблицы и устранение None из результата
    pt = pd.pivot_table(df, "Value", index="Block_Tag", columns="Org_Tag", aggfunc=np.sum)
    pt = pt.fillna(0)

    # сбор и вставка дополнительных колонок-агрегатов
    pt["ПАО"] = pt.apply(lambda row: row["ЦА"] + row["ПЦП"] + row["ТБ"] + row["ВСП"], axis=1)
    pt["ПАО+ДИТ"] = pt.apply(lambda row: row["ПАО"] + row["ДИТ"], axis=1)
    pt["ПАО+ДЗО+ДИТ"] = pt.apply(lambda row: row["ПАО+ДИТ"] + row["ДЗО"], axis=1)

    # перевод названий колонок в латиницу
    pt = pt.rename(columns=ru_en)
    return pt


def df_to_json(df: pd.DataFrame, date: str = "") -> str:
    """
    Переводит датафрейм в кастомный вложенный жсон
    :param df: датафрейм
    :param date: дата, которая подставится в шапку
    :return: вложенное json представление датафрейма
    """

    # хранит в себе всё json-представление датафрейма
    json_view = {"headers": [
        {
            "id": "block",
            "value": "Блок"
        },
        {"id": "fact",
         "value": f"{date} УТВЕРЖДЁННЫЙ БП (ПЛАН + ПРИКАЗЫ)",
         "nested": []
         },
        {"id": "staff",
         "value": f"{date} СКОРР. ПЛАН (ПРЕДЛОЖЕНИЯ БЛОКОВ - численность загружена в АС Simplex)",
         "nested": []
         },
        {"id": "delta",
         "value": f"Дельта {date} СКОРР. ПЛАН - {date} УТВЕРЖДЁННЫЙ БП",
         "nested": []
         }
    ], "rows": []}

    # Создание всех вложенных структур для fact, staff и block с автоматическим id
    # и названием в шапке из упорядоченного словаря ru_en для обеспечения нужного порядка.
    # Comprehension на каждой итерации для того, чтобы списки были независимые без использования deepcopy.
    for i in range(1, len(json_view["headers"])):
        json_view["headers"][i]["nested"] = [{"id": ru_en[elem], "value": elem} for elem in ru_en]

    # проход по строчкам из датафрейма и запись в json
    for i, elem in enumerate(df.iterrows()):
        row = f"row{i}"

        # запись базового элемента строки - её id, название блока, а также значения для свёрнутого вида
        # elem[0] - заголовок строки (название блока)
        # elem[1] - значения колонок
        # elem[1][8, 17, 26] - значения из колонки ПАО+ДЗО+ДИТ для свёрнутого вида
        # TODO: это ненадёжно, нужно по заголовку
        json_view["rows"].append({"id": row, "rowValues": [
            {"id": "block", "value": elem[0]},  # название строки (блока)
            {"id": "fact", "value": elem[1][8], "nested": []},  # данные из плана
            {"id": "staff", "value": elem[1][17], "nested": []},  # данные из предложений
            {"id": "delta", "value": elem[1][26], "nested": []}  # дельта
        ]})

        # работа с nested содержимым строки

        # срезы из строки по трём таблицам (было, предложено, дельта)
        # таблица изначально была объединена, поэтому используются индексы
        slice1 = elem[1][0:9]
        slice2 = elem[1][9:18]
        slice3 = elem[1][18:27]

        # для каждого среза
        # пробегаемся по упорядоченному списку названий столбцов в en_ru
        # название столбца из списка используем как id для элемента строки
        # а также как индекс для series из среза
        # так обеспечивается желаемый порядок столбцов в json
        json_view["rows"][-1]["rowValues"][1]["nested"] = [{"id": str(ind), "value": slice1[ind]} for ind in en_ru]
        json_view["rows"][-1]["rowValues"][2]["nested"] = [{"id": str(ind), "value": slice2[ind]} for ind in en_ru]
        json_view["rows"][-1]["rowValues"][3]["nested"] = [{"id": str(ind), "value": slice3[ind]} for ind in en_ru]
    # переводим в json
    # myconverter для перехода из типов numpy в python
    # ensure_ascii для кириллицы
    json_view = json.dumps(json_view, default=myconverter, ensure_ascii=False)
    return json_view


def myconverter(obj):
    """
    Конвертер из numpy типов в python для json
    :param obj:
    :return:
    """
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()


# df to binary file with excel type
def df_to_binary_excel(df):
    """
    Переводит датафрейм в excel таблицу
    :param df:
    :return:
    """
    towrite = io.BytesIO()
    df.to_excel(towrite, engine='xlsxwriter')  # write to BytesIO buffer
    towrite.seek(0)
    return towrite.getvalue()


def process_xls(table_name: str = "denis_debug.xlsx", excel=False):  # , date="2021-01-01"):
    """
    Достаёт датафрейм из xlsx
    :param table_name: название файла
    :param excel: нужно ли экспортировать в excel
    :return: см process_dataframe
    """
    print("Начал чтение")
    head = pd.read_excel(table_name)
    print("Прочитал")
    return process_dataframe(head, excel=excel)


def add_total_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет строчку с total суммами по столбцам к итоговой таблице
    :param df: финальный датафрейм с тремя конкатенированными таблицами
    :return: датафрейм с добавленной строкой сумм
    """
    sumrow = pd.DataFrame(columns=df.columns)
    indx = sumrow.index
    sumrow = sumrow.append(df.sum(numeric_only=True), ignore_index=True)
    sumrow.index = indx.union(["Total"])
    df = pd.concat([sumrow, df])
    return df


#
#                 \/ Функция внизу \/
# ============================================================
#
def process_dataframe(df: pd.DataFrame, excel: bool = False):
    """
    Читает датафрейм и делает из него сводную таблицу в json
    /или excel - пока не реализовано/

    :param df: датафрейм
    :param excel: Возвращать excel вместо json
    :return: сводная таблица в выбранном формате
    """
    print("Читаю колонки")
    df = pd.DataFrame(df, columns=["Status", "Value", "ValueDate", "Org_Tag", "Block_Tag"])

    # дроп нулей в org_tag и block_tag
    df = df[(df[["Org_Tag", "Block_Tag"]].notnull()).all(axis=1)]

    print("Убираю нули в остальных колонках")
    df = df.fillna(0)

    date = df["ValueDate"].values[0]  # получаю дату
    print("Выборка")

    default = df_to_pivot(df[df["Status"].astype(int) == 0])
    edited = df_to_pivot(df[df["Status"].astype(int) == 1])
    delta = edited - default

    print("Экспорт")

    # EXCEL
    if excel:
        # добавляю заголовки табличек
        default.columns = pd.MultiIndex.from_product(
            [[f"{date} УТВЕРЖДЁННЫЙ БП (ПЛАН + ПРИКАЗЫ)"], default.columns])
        edited.columns = pd.MultiIndex.from_product(
            [[f"{date} СКОРР. ПЛАН (ПРЕДЛОЖЕНИЯ БЛОКОВ - численность загружена в АС Simplex)"], edited.columns])
        delta.columns = pd.MultiIndex.from_product(
            [[f"Дельта {date} СКОРР. ПЛАН - {date} УТВЕРЖДЁННЫЙ БП"], delta.columns])

        # объединяю три таблицы
        summary = pd.concat([default, edited, delta], axis=1, sort=False)
        # добавляю строку с суммами
        summary = add_total_row(summary)
        # обратное переименовывание
        summary = summary.rename(columns=en_ru)

        # summary.to_excel('excel_export.xlsx')
        # exit()
        return df_to_binary_excel(summary)

    # JSON
    summary = pd.concat([default, edited, delta], axis=1, sort=False)
    summary = add_total_row(summary)
    return df_to_json(summary, date=date)


if __name__ == "__main__":
    print(process_xls())
