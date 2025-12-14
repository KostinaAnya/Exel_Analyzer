import pandas as pd
import warnings
from typing import Dict, Optional

# Отключаем предупреждения openpyxl о стилях
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает названия столбцов - безопасная версия."""
    # Преобразуем все названия столбцов в строки, даже если они числа
    new_columns = []
    for col in df.columns:
        col_str = str(col).strip().lower()
        # Заменяем пробелы и специальные символы на подчеркивания
        col_str = col_str.replace(' ', '_').replace(',', '').replace('.', '').replace(':', '')
        new_columns.append(col_str)
    df.columns = new_columns
    return df

def read_excel_with_memory_optimization(
    file_path: str, 
    required_cols: list, 
    header: Optional[int] = None,
    dtype_spec: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Читает Excel файл с оптимизацией памяти.
    """
    print(f"[DEBUG] Чтение файла: {file_path}")
    
    # Читаем весь файл сначала
    df = pd.read_excel(
        file_path,
        engine='openpyxl',
        header=header
    )
    
    print(f"[DEBUG] Исходные столбцы: {list(df.columns)}")
    
    # Очищаем названия столбцов
    df = clean_columns(df)
    
    print(f"[DEBUG] Очищенные столбцы: {list(df.columns)}")
    print(f"[DEBUG] Ищем столбцы: {required_cols}")
    
    # Проверяем наличие обязательных столбцов (в очищенном виде)
    # Сначала тоже очистим имена искомых столбцов
    required_cleaned = []
    for col in required_cols:
        col_clean = str(col).strip().lower()
        col_clean = col_clean.replace(' ', '_').replace(',', '').replace('.', '').replace(':', '')
        required_cleaned.append(col_clean)
    
    print(f"[DEBUG] Очищенные искомые столбцы: {required_cleaned}")
    
    # Проверяем наличие
    missing_cols = []
    for col_clean in required_cleaned:
        if col_clean not in df.columns:
            missing_cols.append(col_clean)
    
    if missing_cols:
        raise KeyError(f"В файле отсутствуют столбцы: {missing_cols}. Доступные столбцы: {list(df.columns)}")
    
    # Оставляем только нужные столбцы
    df = df[required_cleaned]
    
    # Восстанавливаем исходные имена для удобства
    rename_dict = {}
    for original, cleaned in zip(required_cols, required_cleaned):
        rename_dict[cleaned] = original
    df = df.rename(columns=rename_dict)
    
    return df

def analyze_files(file_orders: str, file_revenue: str, file_costs: str) -> pd.DataFrame:
    """
    Анализирует три файла Excel с оптимизацией памяти.
    """
    print(f"[ANALYZER] Начало анализа...")
    
    # 1. ФАЙЛ ЗАКАЗОВ
    print(f"[ANALYZER] Чтение файла заказов...")
    orders_df = read_excel_with_memory_optimization(
        file_path=file_orders,
        required_cols=['артикул', 'статус'],
        header=None  # Пробуем без заголовка, если не работает - поставь 0
    )
    
    # Приводим статусы к нижнему регистру для надежности
    orders_df['статус'] = orders_df['статус'].astype(str).str.lower().str.replace('ё', 'е')
    
    # 2. ФАЙЛ ВЫРУЧКИ
    print(f"[ANALYZER] Чтение файла выручки...")
    revenue_df = read_excel_with_memory_optimization(
        file_path=file_revenue,
        required_cols=['артикул', 'сумма итого, руб.'],
        header=1
    )
    
    # 3. ФАЙЛ ЦЕН
    print(f"[ANALYZER] Чтение файла цен...")
    cost_df = read_excel_with_memory_optimization(
        file_path=file_costs,
        required_cols=['артикул', 'закупочная цена'],
        header=None  # Пробуем без заголовка, если не работает - поставь 0
    )
    
    # Далее стандартная обработка...
    delivered = orders_df[orders_df['статус'].str.contains('доставлен')].groupby('артикул').size()
    cancelled = orders_df[orders_df['статус'].str.contains('отмен')].groupby('артикул').size()
    
    all_articles = pd.Index(orders_df['артикул'].unique(), name='артикул')
    
    delivered = delivered.reindex(all_articles, fill_value=0).astype('int32')
    cancelled = cancelled.reindex(all_articles, fill_value=0).astype('int32')
    
    revenue_sum = revenue_df.groupby('артикул')['сумма итого, руб.'].sum().astype('float32')
    cost_avg = cost_df.groupby('артикул')['закупочная цена'].mean().astype('float32')
    
    report_df = pd.DataFrame({
        'Продано заказов': delivered,
        'Отменено заказов': cancelled
    }, index=all_articles)
    
    report_df = report_df.join(revenue_sum, how='left').fillna(0)
    report_df = report_df.join(cost_avg, how='left')
    report_df = report_df.rename(columns={'закупочная цена': 'закупочная цена за шт'})
    
    report_df['Прибыль'] = report_df['сумма итого, руб.'] - (report_df['Продано заказов'] * report_df['закупочная цена за шт'].fillna(0))
    report_df['Прибыль'] = report_df['Прибыль'].astype('float32')
    
    total_row = pd.DataFrame({
        'Продано заказов': [report_df['Продано заказов'].sum()],
        'Отменено заказов': [report_df['Отменено заказов'].sum()],
        'сумма итого, руб.': [report_df['сумма итого, руб.'].sum()],
        'закупочная цена за шт': [None],
        'Прибыль': [report_df['Прибыль'].sum()]
    }, index=['Итого'])
    
    report_df = pd.concat([report_df, total_row])
    report_df.index.name = 'Артикул'
    
    print(f"[ANALYZER] Анализ завершен!")
    
    return report_df