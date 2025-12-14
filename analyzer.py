import pandas as pd
import warnings
import re

warnings.filterwarnings('ignore')

def find_header_row(file_path, target_columns=['артикул', 'статус'], max_rows=30):
    """
    Находит номер строки, содержащей искомые заголовки.
    """
    # Читаем файл без заголовка, только первые max_rows строк
    df_raw = pd.read_excel(file_path, header=None, nrows=max_rows)
    
    # Ищем строку, содержащую целевые слова в любом регистре
    for idx, row in df_raw.iterrows():
        # Преобразуем все ячейки строки в строки и в нижний регистр
        row_str = ' '.join(str(cell).lower().strip() for cell in row if pd.notna(cell))
        
        # Проверяем, содержатся ли все целевые слова в строке
        found_all = True
        for target in target_columns:
            if target.lower() not in row_str:
                found_all = False
                break
        
        if found_all:
            print(f"[DEBUG] Найдена строка заголовка: строка {idx}")
            # Возвращаем номер строки (в pandas индекс с 0, для header нужно указать этот номер)
            return idx
    
    # Если не нашли, возвращаем None (будем использовать первую строку)
    print(f"[DEBUG] Строка с заголовками не найдена в первых {max_rows} строках")
    return None

def read_excel_with_dynamic_header(file_path, required_cols, max_search=30):
    """
    Читает Excel файл, автоматически находя строку с заголовками.
    """
    print(f"[DEBUG] Поиск заголовков в файле: {file_path}")
    
    # Находим строку с заголовками
    header_row = find_header_row(file_path, required_cols, max_search)
    
    if header_row is None:
        # Если заголовки не найдены, читаем без заголовков и используем первую строку
        print(f"[DEBUG] Заголовки не найдены, читаем без заголовка")
        df = pd.read_excel(file_path, header=None)
        
        # Ищем столбцы по содержимому
        column_mapping = {}
        
        # Просматриваем первые 5 строк для поиска столбцов
        for col_idx in range(min(5, df.shape[0])):
            row_sample = df.iloc[col_idx]
            for i, cell in enumerate(row_sample):
                if pd.notna(cell):
                    cell_str = str(cell).lower().strip()
                    if any(target in cell_str for target in required_cols):
                        # Нашли столбец с одним из целевых слов
                        for target in required_cols:
                            if target in cell_str:
                                column_mapping[target] = i
                                break
        
        if len(column_mapping) == len(required_cols):
            print(f"[DEBUG] Найдены столбцы: {column_mapping}")
            # Оставляем только нужные столбцы
            df = df[[column_mapping[col] for col in required_cols]]
            df.columns = required_cols
            
            # Удаляем строку с заголовками из данных
            if header_row is not None:
                df = df.drop(header_row).reset_index(drop=True)
        else:
            missing = [col for col in required_cols if col not in column_mapping]
            raise KeyError(f"Не удалось найти столбцы: {missing}")
    else:
        # Читаем с найденной строкой заголовка
        df = pd.read_excel(file_path, header=header_row)
        
        # Приводим названия столбцов к нижнему регистру и очищаем
        df.columns = [str(col).strip().lower() for col in df.columns]
    
    return df

def analyze_files(file_orders, file_revenue, file_costs):
    """
    Анализирует три файла Excel с автоматическим определением заголовков.
    """
    print(f"[ANALYZER] Начало анализа с динамическими заголовками")
    
    # 1. ФАЙЛ ЗАКАЗОВ
    print(f"[ANALYZER] Чтение файла заказов...")
    orders_df = read_excel_with_dynamic_header(
        file_path=file_orders,
        required_cols=['артикул', 'статус'],
        max_search=10
    )
    
    # Проверяем и переименовываем столбцы
    orders_df.columns = [str(col).strip().lower() for col in orders_df.columns]
    
    # Ищем столбцы по подстроке
    art_col = None
    status_col = None
    
    for col in orders_df.columns:
        col_lower = str(col).lower()
        if 'артикул' in col_lower:
            art_col = col
        elif 'статус' in col_lower:
            status_col = col
    
    if art_col is None or status_col is None:
        # Если не нашли по названиям, берем первые два столбца
        print(f"[WARNING] Не найдены точные названия столбцов. Использую первые два столбца.")
        art_col = orders_df.columns[0]
        status_col = orders_df.columns[1]
    
    # Переименовываем для удобства
    orders_df = orders_df.rename(columns={
        art_col: 'артикул',
        status_col: 'статус'
    })
    
    print(f"[DEBUG] Столбцы заказов: {list(orders_df.columns)}")
    print(f"[DEBUG] Первые строки заказов:")
    print(orders_df.head())
    
    # 2. ФАЙЛ ВЫРУЧКИ (уже имеет заголовок во второй строке)
    print(f"[ANALYZER] Чтение файла выручки...")
    revenue_df = pd.read_excel(file_revenue, header=1)
    revenue_df.columns = [str(col).strip().lower() for col in revenue_df.columns]
    
    # Ищем столбец выручки
    revenue_col = None
    for col in revenue_df.columns:
        if 'сумма' in str(col).lower() and 'итого' in str(col).lower():
            revenue_col = col
            break
    
    if revenue_col:
        revenue_df = revenue_df[['артикул', revenue_col]]
        revenue_df = revenue_df.rename(columns={revenue_col: 'сумма итого, руб.'})
    else:
        # Берем второй столбец как выручку
        revenue_df = revenue_df.iloc[:, [0, 1]]
        revenue_df.columns = ['артикул', 'сумма итого, руб.']
    
    # 3. ФАЙЛ ЗАКУПОЧНЫХ ЦЕН
    print(f"[ANALYZER] Чтение файла цен...")
    cost_df = read_excel_with_dynamic_header(
        file_path=file_costs,
        required_cols=['артикул', 'закупочная цена'],
        max_search=10
    )
    
    cost_df.columns = [str(col).strip().lower() for col in cost_df.columns]
    
    # Ищем столбцы в файле цен
    cost_col = None
    for col in cost_df.columns:
        if 'закупоч' in str(col).lower() or 'цена' in str(col).lower():
            cost_col = col
            break
    
    if cost_col:
        cost_df = cost_df[['артикул', cost_col]]
        cost_df = cost_df.rename(columns={cost_col: 'закупочная цена'})
    else:
        cost_df = cost_df.iloc[:, [0, 1]]
        cost_df.columns = ['артикул', 'закупочная цена']
    
    # 4. ДАЛЬНЕЙШАЯ ОБРАБОТКА
    # Приводим статусы к нижнему регистру
    orders_df['статус'] = orders_df['статус'].astype(str).str.lower().str.replace('ё', 'е')
    
    # Группируем
    delivered = orders_df[orders_df['статус'].str.contains('доставлен')].groupby('артикул').size()
    cancelled = orders_df[orders_df['статус'].str.contains('отмен')].groupby('артикул').size()
    
    all_articles = pd.Index(orders_df['артикул'].dropna().unique(), name='артикул')
    
    delivered = delivered.reindex(all_articles, fill_value=0).astype('int32')
    cancelled = cancelled.reindex(all_articles, fill_value=0).astype('int32')
    
    revenue_sum = revenue_df.groupby('артикул')['сумма итого, руб.'].sum().astype('float32')
    cost_avg = cost_df.groupby('артикул')['закупочная цена'].mean().astype('float32')
    
    # Формируем отчет
    report_df = pd.DataFrame({
        'Продано заказов': delivered,
        'Отменено заказов': cancelled
    }, index=all_articles)
    
    report_df = report_df.join(revenue_sum, how='left').fillna(0)
    report_df = report_df.join(cost_avg, how='left')
    report_df = report_df.rename(columns={'закупочная цена': 'закупочная цена за шт'})
    
    report_df['Прибыль'] = report_df['сумма итого, руб.'] - (report_df['Продано заказов'] * report_df['закупочная цена за шт'].fillna(0))
    report_df['Прибыль'] = report_df['Прибыль'].astype('float32')
    
    # Итоговая строка
    total_row = pd.DataFrame({
        'Продано заказов': [report_df['Продано заказов'].sum()],
        'Отменено заказов': [report_df['Отменено заказов'].sum()],
        'сумма итого, руб.': [report_df['сумма итого, руб.'].sum()],
        'закупочная цена за шт': [None],
        'Прибыль': [report_df['Прибыль'].sum()]
    }, index=['Итого'])
    
    report_df = pd.concat([report_df, total_row])
    report_df.index.name = 'Артикул'
    
    print(f"[ANALYZER] Анализ завершен успешно!")
    
    return report_df