# analyzer.py
import pandas as pd

def clean_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def analyze_files(file_orders, file_revenue, file_costs):
    """
    Анализирует три файла Excel и возвращает отчетный DataFrame
    """
    # Загрузка файла заказов
    orders_df = pd.read_excel(file_orders, engine='openpyxl', index_col=None)
    orders_df = clean_columns(orders_df)

    required_orders = ['артикул', 'статус']
    for col in required_orders:
        if col not in orders_df.columns:
            raise KeyError(f"В файле заказов отсутствует столбец '{col}'")

    delivered = orders_df[orders_df['статус'] == 'Доставлен'].groupby('артикул').size()
    cancelled = orders_df[orders_df['статус'] == 'Отменён'].groupby('артикул').size()

    all_articles = orders_df['артикул'].unique()
    delivered = delivered.reindex(all_articles, fill_value=0)
    cancelled = cancelled.reindex(all_articles, fill_value=0)

    # Загрузка файла выручки
    revenue_df = pd.read_excel(file_revenue, engine='openpyxl', index_col=None, header=1)
    revenue_df = clean_columns(revenue_df)

    required_revenue = ['артикул', 'сумма итого, руб.']
    for col in required_revenue:
        if col not in revenue_df.columns:
            raise KeyError(f"В файле выручки отсутствует столбец '{col}'")
    revenue_sum = revenue_df.groupby('артикул')['сумма итого, руб.'].sum()

    # Загрузка файла с закупочными ценами
    cost_df = pd.read_excel(file_costs, engine='openpyxl', index_col=None)
    cost_df = clean_columns(cost_df)
                              
    required_costs = ['артикул', 'закупочная цена']
    for col in required_costs:
        if col not in cost_df.columns:
            raise KeyError(f"В файле с закупочными ценами отсутствует столбец '{col}'")

    # Берём закупочную цену по артикулу (если несколько строк для артикула - берем среднее или первую цену)
    cost_avg = cost_df.groupby('артикул')['закупочная цена'].mean()

    # Формируем итоговый датафрейм
    report_df = pd.DataFrame({
        'Продано заказов': delivered,
        'Отменено заказов': cancelled
    })

    # Добавляем выручку
    report_df = report_df.join(revenue_sum, how='left').fillna(0)

    # Добавляем закупочную цену
    report_df = report_df.join(cost_avg, how='left')

    # Рассчитываем прибыль по формуле: прибыль = выручка - (кол-во доставленных заказов * закупочная цена)
    report_df['Прибыль'] = report_df['сумма итого, руб.'] - (report_df['Продано заказов'] * report_df['закупочная цена'])

    # Добавляем итоговую строку с суммами
    total_row = pd.DataFrame({
        'Продано заказов': [report_df['Продано заказов'].sum()],
        'Отменено заказов': [report_df['Отменено заказов'].sum()],
        'сумма итого, руб.': [report_df['сумма итого, руб.'].sum()],
        'Прибыль': [report_df['Прибыль'].sum()]
    }, index=['Итого'])
    
    report_df = report_df.rename(columns={'закупочная цена': 'закупочная цена за шт'})
    report_df = pd.concat([report_df, total_row])

    return report_df