# app.py
from flask import Flask, render_template, request, send_file, flash
import os
from werkzeug.utils import secure_filename
import pandas as pd
from io import BytesIO
from analyzer import analyze_files  # Импортируем функцию из нового файла

app = Flask(__name__)
app.config['SECRET_KEY'] = '59ccb0fef601d35a2fbacc41dd5f6ec1563a8d04c53e14db3ee2fdc770de7afe'  # Сгенерируйте новый ключ
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

# Создаем папку для загрузок
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Проверяем, что все три файла загружены
        if ('file_orders' not in request.files or 
            'file_revenue' not in request.files or 
            'file_costs' not in request.files):
            flash('Пожалуйста, загрузите все три файла')
            return render_template('index.html')
        
        file_orders = request.files['file_orders']
        file_revenue = request.files['file_revenue']
        file_costs = request.files['file_costs']
        
        # Проверяем, что файлы выбраны
        if (file_orders.filename == '' or 
            file_revenue.filename == '' or 
            file_costs.filename == ''):
            flash('Пожалуйста, выберите все три файла')
            return render_template('index.html')
        
        # Проверяем расширения файлов
        if (not allowed_file(file_orders.filename) or 
            not allowed_file(file_revenue.filename) or 
            not allowed_file(file_costs.filename)):
            flash('Разрешены только файлы Excel (.xlsx, .xls)')
            return render_template('index.html')
        
        try:
            # Сохраняем файлы временно
            orders_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file_orders.filename))
            revenue_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file_revenue.filename))
            costs_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file_costs.filename))
            
            file_orders.save(orders_path)
            file_revenue.save(revenue_path)
            file_costs.save(costs_path)
            
            # Выполняем анализ с помощью функции из analyzer.py
            result_df = analyze_files(orders_path, revenue_path, costs_path)
            
            # Создаем Excel файл в памяти
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result_df.to_excel(writer, sheet_name='Отчет')
            
            output.seek(0)
            
            # Удаляем временные файлы
            os.remove(orders_path)
            os.remove(revenue_path)
            os.remove(costs_path)
            
            # Отправляем файл пользователю
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='отчет_по_артикулам_с_прибылью.xlsx'
            )
            
        except KeyError as e:
            flash(f'Ошибка в структуре файла: {str(e)}')
        except Exception as e:
            flash(f'Произошла ошибка при обработке: {str(e)}')
        
        # Если произошла ошибка, удаляем временные файлы
        for path in [orders_path, revenue_path, costs_path]:
            if os.path.exists(path):
                os.remove(path)
    
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)