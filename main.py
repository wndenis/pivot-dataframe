import numpy as np
import pandas as pd
import json
import io


def df_to_pivot(df):
    """
    Делает сводную таблицу для датафрейма с агрегацией ПАО и проч.
    :param df: датафрейм
    :return: сводный датафрейм
    """
    pt = pd.pivot_table(df, "Value", index="Block_Tag", columns="Org_Tag", aggfunc=np.sum)
    pt = pt.fillna(0)
    pt["ПАО"] = pt.apply(lambda row: row["ЦА"] + row["ПЦП"] + row["ТБ"], axis=1)
    pt["ПАО+ДИТ"] = pt.apply(lambda row: row["ПАО"] + row["ДИТ"], axis=1)
    pt["ПАО+ДЗО+ДИТ"] = pt.apply(lambda row: row["ПАО+ДИТ"] + row["ДЗО"], axis=1)
    names = {"ПАО": "PAO",
             "ЦА": "CA",
             "ПЦП": "PCP",
             "ТБ": "TB",
             "ДИТ": "DIT",
             "ДЗО": "DZO",
             "ПАО+ДИТ": "PAO+DIT",
             "ПАО+ДЗО+ДИТ": "PAO+DZO+DIT"
             }
    pt = pt.rename(columns=names)
    pt = pt.fillna(0)
    return pt


def df_to_json(df: pd.DataFrame, date: str = ""):
    """
    Переводит датафрейм в кастомный вложенный жсон
    :param df: датафрейм
    :param date: дата, которая подставится в шапку
    :return: вложенное json представление датафрейма
    """
    json_view = {"headers": [
        {
            "id": "block",
            "value": "Блок"
        },
        {"id": "fact",
         "value": f"{date} УТВЕРЖДЁННЫЙ БП (ПЛАН + ПРИКАЗЫ)",
         "nested": [
             {
                 "id": "PAO+DZO+DIT",
                 "value": "ПАО+ДЗО+ДИТ"
             },
             {
                 "id": "PAO+DIT",
                 "value": "ПАО+ДИТ"
             },
             {
                 "id": "PAO",
                 "value": "ПАО"
             },
             {
                 "id": "CA",
                 "value": "ЦА"
             },
             {
                 "id": "PCP",
                 "value": "ПЦП"
             },
             {
                 "id": "TB",
                 "value": "ТБ"
             },
             {
                 "id": "DIT",
                 "value": "ДИТ"
             },
             {
                 "id": "DZO",
                 "value": "ДЗО"
             },
             {
                 "id": "ВСП",  # todo: сделать английский ИД
                 "value": "ВСП"
             }
         ]
        },
        {"id": "staff",
         "value": f"{date} СКОРР. ПЛАН (ПРЕДЛОЖЕНИЯ БЛОКОВ - численность загружена в АС Simplex)",
         "nested": [
             {
                 "id": "PAO+DZO+DIT",
                 "value": "ПАО+ДЗО+ДИТ"
             },
             {
                 "id": "PAO+DIT",
                 "value": "ПАО+ДИТ"
             },
             {
                 "id": "PAO",
                 "value": "ПАО"
             },
             {
                 "id": "CA",
                 "value": "ЦА"
             },
             {
                 "id": "PCP",
                 "value": "ПЦП"
             },
             {
                 "id": "TB",
                 "value": "ТБ"
             },
             {
                 "id": "DIT",
                 "value": "ДИТ"
             },
             {
                 "id": "DZO",
                 "value": "ДЗО"
             }
         ]
         },
        {"id": "delta",
         "value": f"Дельта {date} СКОРР. ПЛАН - {date} УТВЕРЖДЁННЫЙ БП",
         "nested": [
             {
                 "id": "PAO+DZO+DIT",
                 "value": "ПАО+ДЗО+ДИТ"
             },
             {
                 "id": "PAO+DIT",
                 "value": "ПАО+ДИТ"
             },
             {
                 "id": "PAO",
                 "value": "ПАО"
             },
             {
                 "id": "CA",
                 "value": "ЦА"
             },
             {
                 "id": "PCP",
                 "value": "ПЦП"
             },
             {
                 "id": "TB",
                 "value": "ТБ"
             },
             {
                 "id": "DIT",
                 "value": "ДИТ"
             },
             {
                 "id": "DZO",
                 "value": "ДЗО"
             }
         ]
         }

    ], "rows": []}

    i = 0
    for elem in df.iterrows():
        row = f"row{i}"
        json_view["rows"].append({"id": row, "rowValues": [
            {"id": "block", "value": elem[0]},
            {"id": "fact", "value": elem[1][8], "nested": []},
            {"id": "staff", "value": elem[1][17], "nested": []},
            {"id": "delta", "value": elem[1][26], "nested": []}
        ]})

        slice1 = elem[1][0:9]
        slice2 = elem[1][9:18]
        slice3 = elem[1][18:27]

        slice1 = list(zip(slice1, slice1.index))
        slice2 = list(zip(slice2, slice2.index))
        slice3 = list(zip(slice3, slice3.index))

        json_view["rows"][-1]["rowValues"][1]["nested"] = [{"id": str(ind), "value": val} for val, ind in slice1]
        json_view["rows"][-1]["rowValues"][2]["nested"] = [{"id": str(ind), "value": val} for val, ind in slice2]
        json_view["rows"][-1]["rowValues"][3]["nested"] = [{"id": str(ind), "value": val} for val, ind in slice3]
        i += 1
    json_view = json.dumps(json_view, default=myconverter, ensure_ascii=False)#, indent=2)
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


def process_xls(table_name: str = "denis_input_data.xlsx", excel=False): #, date="2021-01-01"):
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


def add_total_row(df: pd.DataFrame):
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



###
###                 \/ Функция внизу \/
### ============================================================
###
def process_dataframe(df: pd.DataFrame, excel: bool=False):
    """
    Читает датафрейм и делает из него сводную таблицу в json
    /или excel - пока не реализовано/

    :param df: датафрейм
    :param excel: Возвращать excel вместо json
    :return: сводная таблица в выбранном формате
    """
    print("Читаю колонки")
    df = pd.DataFrame(df, columns=["Status", "Value", "ValueDate", "Org_Tag", "Block_Tag"])
    print("Убираю нули")

    df = df.fillna(0)
    # if date:
    #     head = head[head["ValueDate"] == date]
    date = df["ValueDate"].values[0]  # получаю дату
    print("Выборка")

    # FIXME: числа были в строках
    default = df_to_pivot(df[df["Status"].isin((0, "0"))])
    edited = df_to_pivot(df[df["Status"].isin((1, "1"))])
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
        # summary.to_excel('excel_export.xlsx')
        return df_to_binary_excel(summary)

    # JSON
    summary = pd.concat([default, edited, delta], axis=1, sort=False)
    summary = add_total_row(summary)
    return df_to_json(summary, date=date)


if __name__ == "__main__":
    print(process_xls())
