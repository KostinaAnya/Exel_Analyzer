import pandas as pd
import warnings
from typing import Dict, Optional

# Отключаем предупреждения openpyxl о стилях
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает названия столбцов - безопасная версия."""
    # Преобразуем все названия столбцов в строки, даже если они числа
    df.columns = [str(col).strip().lower() for col in df.columns]
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
    # Устанавливаем умолчательные типы данных для оптимизации
    default_dtype = {
        'артикул': 'str',
        'статус': 'category',
        'сумма итого, руб.': 'float32',
        'закупочная цена': 'float32'
    }
    
    if dtype_spec:
        default_dtype.update(dtype_spec)
    
    use_cols = [col for col in required_cols if col in default_dtype]
    final_dtype = {col: default_dtype[col] for col in use_cols if col in default_dtype}
    
    try:
        df = pd.read_excel(
            file_path,
            engine='openpyxl',
            header=header,
            usecols=required_cols,
            dtype=final_dtype
        )
    except ValueError as e:
        df = pd.read_excel(
            file_path,
            engine='openpyxl',
            header=header
        )
        df = clean_columns(df)
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"В файле отсутствуют столбцы: {missing_cols}")
        
        df = df[required_cols]
        for col, dtype in final_dtype.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype, errors='ignore')
    
    df = clean_columns(df)
    return df

def analyze_files(file_orders: str, file_revenue: str, file_costs: str) -> pd.DataFrame:
    """
    Анализирует три файла Excel с оптимизацией памяти.
    """
    print(f"[DEBUG] Начинаем анализ...")
    
    # 1. Обработка файла заказов
    orders_df = read_excel_with_memory_optimization(
        file_path=file_orders,
        required_cols=['артикул', 'статус'],
        dtype_spec={'артикул': 'str', 'статус': 'category'}
    )
    
    if 'артикул' not in orders_df.columns or 'статус' not in orders_df.columns:
        raise KeyError("В файле заказов отсутствуют обязательные столбцы")
    
    delivered = orders_df[orders_df['статус'] == 'Доставлен'].groupby('артикул').size()
    cancelled = orders_df[orders_df['статус'] == 'Отменён'].groupby('артикул').size()
    
    all_articles = pd.Index(orders_df['артикул'].unique(), name='артикул')
    del orders_df
    
    delivered = delivered.reindex(all_articles, fill_value=0).astype('int32')
    cancelled = cancelled.reindex(all_articles, fill_value=0).astype('int32')
    
    # 2. Обработка файла выручки
    revenue_df = read_excel_with_memory_optimization(
        file_path=file_revenue,
        required_cols=['артикул', 'сумма итого, руб.'],
        header=1,
        dtype_spec={'артикул': 'str', 'сумма итого, руб.': 'float32'}
    )
    
    revenue_sum = revenue_df.groupby('артикул')['сумма итого, руб.'].sum().astype('float32')
    del revenue_df
    
    # 3. Обработка файла закупочных цен
    cost_df = read_excel_with_memory_optimization(
        file_path=file_costs,
        required_cols=['артикул', 'закупочная цена'],
        dtype_spec={'артикул': 'str', 'закупочная цена': 'float32'}
    )
    
    cost_avg = cost_df.groupby('артикул')['закупочная цена'].mean().astype('float32')
    del cost_df
    
    # 4. Формирование отчета
    report_df = pd.DataFrame({
        'Продано заказов': delivered,
        'Отменено заказов': cancelled
    }, index=all_articles)
    
    report_df = report_df.join(revenue_sum, how='left').fillna(0)
    report_df = report_df.join(cost_avg, how='left')
    report_df = report_df.rename(columns={'закупочная цена': 'закупочная цена за шт'})
    
    report_df['Прибыль'] = report_df['сумма итого, руб.'] - (report_df['Продано заказов'] * report_df['закупочная цена за шт'].fillna(0))
    report_df['Прибыль'] = report_df['Прибыль'].astype('float32')
    
    # 5. Итоговая строка
    total_row = pd.DataFrame({
        'Продано заказов': [report_df['Продано заказов'].sum()],
        'Отменено заказов': [report_df['Отменено заказов'].sum()],
        'сумма итого, руб.': [report_df['сумма итого, руб.'].sum()],
        'закупочная цена за шт': [None],
        'Прибыль': [report_df['Прибыль'].sum()]
    }, index=['Итого'])
    
    report_df = pd.concat([report_df, total_row])
    report_df.index.name = 'Артикул'
    
    print(f"[DEBUG] Анализ завершен успешно!")
    
    return report_df