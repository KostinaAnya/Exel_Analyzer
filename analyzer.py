import pandas as pd
import warnings

warnings.filterwarnings('ignore')

def analyze_files(file_orders, file_revenue, file_costs):
    """
    Упрощённая и надёжная версия анализатора.
    Читает файлы БЕЗ автоматического определения заголовков.
    """
    print("[ANALYZER] Запуск упрощённой версии")
    
    try:
        # 1. ФАЙЛ ЗАКАЗОВ - читаем БЕЗ заголовков
        print("[1/3] Чтение файла заказов...")
        orders_df = pd.read_excel(file_orders, header=None, engine='openpyxl')
        
        # Ищем строку с заголовками (содержит "артикул" и "статус")
        header_row = None
        for i in range(min(10, len(orders_df))):  # Проверяем первые 10 строк
            row_text = ' '.join(str(cell).lower() for cell in orders_df.iloc[i] if pd.notna(cell))
            if 'артикул' in row_text and 'статус' in row_text:
                header_row = i
                print(f"  Найдены заголовки в строке {i+1}")
                break
        
        if header_row is not None:
            # Используем найденную строку как заголовки
            orders_df.columns = orders_df.iloc[header_row]
            orders_df = orders_df.iloc[header_row+1:].reset_index(drop=True)
        else:
            # Если не нашли - используем первые два столбца
            print("  Заголовки не найдены, использую первые два столбца")
            orders_df = orders_df.iloc[:, :2]
            orders_df.columns = ['артикул', 'статус']
        
        # Очистка названий столбцов
        orders_df.columns = [str(col).strip().lower() for col in orders_df.columns]
        print(f"  Столбцы: {list(orders_df.columns)}")
        
        # 2. ФАЙЛ ВЫРУЧКИ - всегда header=1 (вторая строка)
        print("[2/3] Чтение файла выручки...")
        try:
            revenue_df = pd.read_excel(file_revenue, header=1, engine='openpyxl')
        except:
            # Если не работает header=1, пробуем без заголовка
            revenue_df = pd.read_excel(file_revenue, header=None, engine='openpyxl')
            revenue_df.columns = ['артикул', 'сумма']
        
        revenue_df.columns = [str(col).strip().lower() for col in revenue_df.columns]
        
        # Ищем столбец с выручкой
        revenue_col = None
        for col in revenue_df.columns:
            if 'сумма' in str(col).lower():
                revenue_col = col
                break
        
        if revenue_col:
            revenue_df = revenue_df[['артикул', revenue_col]]
            revenue_df.columns = ['артикул', 'сумма итого, руб.']
        else:
            # Берём второй столбец как выручку
            revenue_df = revenue_df.iloc[:, :2]
            revenue_df.columns = ['артикул', 'сумма итого, руб.']
        
        # 3. ФАЙЛ ЦЕН - читаем БЕЗ заголовков
        print("[3/3] Чтение файла цен...")
        cost_df = pd.read_excel(file_costs, header=None, engine='openpyxl')
        
        # Ищем строку с заголовками
        cost_header = None
        for i in range(min(10, len(cost_df))):
            row_text = ' '.join(str(cell).lower() for cell in cost_df.iloc[i] if pd.notna(cell))
            if 'артикул' in row_text and ('закупоч' in row_text or 'цена' in row_text):
                cost_header = i
                print(f"  Найдены заголовки в строке {i+1}")
                break
        
        if cost_header is not None:
            cost_df.columns = cost_df.iloc[cost_header]
            cost_df = cost_df.iloc[cost_header+1:].reset_index(drop=True)
        else:
            cost_df = cost_df.iloc[:, :2]
            cost_df.columns = ['артикул', 'закупочная цена']
        
        cost_df.columns = [str(col).strip().lower() for col in cost_df.columns]
        
        # 4. ОБРАБОТКА ДАННЫХ
        print("[ANALYZER] Обработка данных...")
        
        # Приводим данные к правильным типам
        orders_df['артикул'] = orders_df['артикул'].astype(str).str.strip()
        orders_df['статус'] = orders_df['статус'].astype(str).str.strip().str.lower()
        
        # Считаем доставленные и отменённые
        delivered_mask = orders_df['статус'].str.contains('доставлен')
        cancelled_mask = orders_df['статус'].str.contains('отмен')
        
        delivered = orders_df[delivered_mask].groupby('артикул').size()
        cancelled = orders_df[cancelled_mask].groupby('артикул').size()
        
        # Все уникальные артикулы
        all_articles = pd.Index(orders_df['артикул'].unique(), name='артикул')
        
        # Реиндексируем
        delivered = delivered.reindex(all_articles, fill_value=0).astype('int32')
        cancelled = cancelled.reindex(all_articles, fill_value=0).astype('int32')
        
        # Выручка
        revenue_df['артикул'] = revenue_df['артикул'].astype(str).str.strip()
        revenue_sum = revenue_df.groupby('артикул')['сумма итого, руб.'].sum().astype('float32')
        
        # Цены
        cost_df['артикул'] = cost_df['артикул'].astype(str).str.strip()
        cost_avg = cost_df.groupby('артикул')['закупочная цена'].mean().astype('float32')
        
        # 5. ФОРМИРОВАНИЕ ОТЧЕТА
        print("[ANALYZER] Формирование отчета...")
        
        report_df = pd.DataFrame({
            'Продано заказов': delivered,
            'Отменено заказов': cancelled
        }, index=all_articles)
        
        report_df = report_df.join(revenue_sum, how='left').fillna(0)
        report_df = report_df.join(cost_avg, how='left')
        report_df = report_df.rename(columns={'закупочная цена': 'закупочная цена за шт'})
        
        # Прибыль
        report_df['Прибыль'] = report_df['сумма итого, руб.'] - (
            report_df['Продано заказов'] * report_df['закупочная цена за шт'].fillna(0)
        )
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
        
        print("[ANALYZER] Анализ успешно завершён!")
        return report_df
        
    except Exception as e:
        # Логируем ошибку подробно
        print(f"[ERROR] Критическая ошибка в analyze_files: {str(e)}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        raise  # Пробрасываем ошибку дальше