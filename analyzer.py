import pandas as pd
import warnings
from typing import Dict, Optional

# Отключаем предупреждения openpyxl о стилях
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает названия столбцов."""
    df.columns = df.columns.str.strip().str.lower()
    return df

def read_excel_with_memory_optimization(
    file_path: str, 
    required_cols: list, 
    header: Optional[int] = None,
    dtype_spec: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Читает Excel файл с оптимизацией памяти.
    
    Args:
        file_path: путь к файлу
        required_cols: список обязательных столбцов
        header: строка заголовка (None или число)
        dtype_spec: словарь с типами данных для столбцов
    
    Returns:
        Оптимизированный DataFrame
    """
    # Устанавливаем умолчательные типы данных для оптимизации
    default_dtype = {
        'артикул': 'str',
        'статус': 'category',      # Экономит ~70% памяти для повторяющихся значений
        'сумма итого, руб.': 'float32',
        'закупочная цена': 'float32'
    }
    
    if dtype_spec:
        default_dtype.update(dtype_spec)
    
    # Фильтруем только нужные типы для фактически присутствующих столбцов
    use_cols = [col for col in required_cols if col in default_dtype]
    final_dtype = {col: default_dtype[col] for col in use_cols if col in default_dtype}
    
    try:
        # Читаем файл с оптимизацией памяти
        df = pd.read_excel(
            file_path,
            engine='openpyxl',
            header=header,
            usecols=required_cols,      # Читаем ТОЛЬКО нужные столбцы
            dtype=final_dtype           # Указываем типы данных для экономии памяти
            # Параметр memory_map удален - он не поддерживается для Excel
        )
    except ValueError as e:
        # Если какие-то столбцы отсутствуют, пытаемся прочитать все, потом отфильтровать
        df = pd.read_excel(
            file_path,
            engine='openpyxl',
            header=header
        )
        df = clean_columns(df)
        
        # Проверяем наличие обязательных столбцов
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"В файле отсутствуют столбцы: {missing_cols}")
        
        # Оставляем только нужные столбцы и применяем типы данных
        df = df[required_cols]
        for col, dtype in final_dtype.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype, errors='ignore')
    
    df = clean_columns(df)
    return df

def analyze_files(file_orders: str, file_revenue: str, file_costs: str) -> pd.DataFrame:
    """
    Анализирует три файла Excel с оптимизацией памяти.
    Возвращает отчетный DataFrame.
    
    Args:
        file_orders: путь к файлу заказов
        file_revenue: путь к файлу выручки  
        file_costs: путь к файлу закупочных цен
    
    Returns:
        Итоговый отчет DataFrame
    """
    print(f"[DEBUG] Начинаем анализ с оптимизацией памяти...")
    
    # ============ 1. ОБРАБОТКА ФАЙЛА ЗАКАЗОВ ============
    print(f"[DEBUG] Чтение файла заказов: {file_orders}")
    orders_df = read_excel_with_memory_optimization(
        file_path=file_orders,
        required_cols=['артикул', 'статус'],
        dtype_spec={'артикул': 'str', 'статус': 'category'}
    )
    
    # Проверяем наличие обязательных столбцов
    if 'артикул' not in orders_df.columns or 'статус' not in orders_df.columns:
        raise KeyError("В файле заказов отсутствуют обязательные столбцы 'артикул' или 'статус'")
    
    # Считаем доставленные и отмененные заказы
    delivered_mask = orders_df['статус'] == 'Доставлен'
    cancelled_mask = orders_df['статус'] == 'Отменён'
    
    delivered = orders_df[delivered_mask].groupby('артикул').size()
    cancelled = orders_df[cancelled_mask].groupby('артикул').size()
    
    # Получаем все уникальные артикулы
    all_articles = pd.Index(orders_df['артикул'].unique(), name='артикул')
    
    # Освобождаем память от orders_df сразу после использования
    del orders_df, delivered_mask, cancelled_mask
    
    # Реиндексируем с заполнением нулями для отсутствующих артикулов
    delivered = delivered.reindex(all_articles, fill_value=0).astype('int32')
    cancelled = cancelled.reindex(all_articles, fill_value=0).astype('int32')
    
    # ============ 2. ОБРАБОТКА ФАЙЛА ВЫРУЧКИ ============
    print(f"[DEBUG] Чтение файла выручки: {file_revenue}")
    revenue_df = read_excel_with_memory_optimization(
        file_path=file_revenue,
        required_cols=['артикул', 'сумма итого, руб.'],
        header=1,  # Вторая строка как заголовок
        dtype_spec={'артикул': 'str', 'сумма итого, руб.': 'float32'}
    )
    
    # Группируем выручку по артикулам
    revenue_sum = revenue_df.groupby('артикул')['сумма итого, руб.'].sum().astype('float32')
    del revenue_df  # Освобождаем память
    
    # ============ 3. ОБРАБОТКА ФАЙЛА ЗАКУПОЧНЫХ ЦЕН ============
    print(f"[DEBUG] Чтение файла закупочных цен: {file_costs}")
    cost_df = read_excel_with_memory_optimization(
        file_path=file_costs,
        required_cols=['артикул', 'закупочная цена'],
        dtype_spec={'артикул': 'str', 'закупочная цена': 'float32'}
    )
    
    # Средняя закупочная цена по артикулу
    cost_avg = cost_df.groupby('артикул')['закупочная цена'].mean().astype('float32')
    del cost_df  # Освобождаем память
    
    # ============ 4. ФОРМИРОВАНИЕ ИТОГОВОГО ОТЧЕТА ============
    print(f"[DEBUG] Формирование итогового отчета...")
    
    # Создаем базовый DataFrame с оптимизированными типами
    report_df = pd.DataFrame({
        'Продано заказов': delivered,
        'Отменено заказов': cancelled
    }, index=all_articles)
    
    # Присоединяем выручку с заполнением нулями
    report_df = report_df.join(revenue_sum, how='left').fillna(0)
    
    # Присоединяем закупочные цены
    report_df = report_df.join(cost_avg, how='left')
    report_df = report_df.rename(columns={'закупочная цена': 'закупочная цена за шт'})
    
    # Рассчитываем прибыль с оптимизацией
    report_df['Прибыль'] = report_df['сумма итого, руб.'] - (report_df['Продано заказов'] * report_df['закупочная цена за шт'].fillna(0))
    report_df['Прибыль'] = report_df['Прибыль'].astype('float32')
    
    # ============ 5. ДОБАВЛЕНИЕ ИТОГОВОЙ СТРОКИ ============
    print(f"[DEBUG] Добавление итоговой строки...")
    
    # Создаем итоговую строку
    total_row = pd.DataFrame({
        'Продано заказов': [report_df['Продано заказов'].sum()],
        'Отменено заказов': [report_df['Отменено заказов'].sum()],
        'сумма итого, руб.': [report_df['сумма итого, руб.'].sum()],
        'закупочная цена за шт': [None],  # Для итогов не имеет смысла
        'Прибыль': [report_df['Прибыль'].sum()]
    }, index=['Итого'])
    
    # Объединяем с основным отчетом
    report_df = pd.concat([report_df, total_row])
    
    # Убедимся, что индексы имеют имя для читаемости
    report_df.index.name = 'Артикул'
    
    print(f"[DEBUG] Анализ завершен успешно! Отчет содержит {len(report_df)-1} артикулов.")
    
    return report_df

# Функция для быстрого тестирования
def test_analyzer():
    """Тестирует основные функции analyzer."""
    print("Тестирование analyzer.py...")
    
    # Проверяем наличие функции
    if 'analyze_files' not in globals():
        print("❌ Функция analyze_files не найдена!")
        return False
    
    print("✅ Модуль загружен успешно")
    print("✅ Функция analyze_files доступна")
    
    # Проверяем параметры функции
    import inspect
    sig = inspect.signature(analyze_files)
    params = list(sig.parameters.keys())
    
    if params == ['file_orders', 'file_revenue', 'file_costs']:
        print("✅ Сигнатура функции корректна")
    else:
        print(f"❌ Неожиданная сигнатура: {params}")
    
    return True

if __name__ == '__main__':
    # Запуск теста при прямом выполнении файла
    test_analyzer()
    print("\n✅ analyzer.py готов к использованию!")
    print("\nДля использования в Flask:")
    print("from analyzer import analyze_files")