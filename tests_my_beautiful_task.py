from os import walk, path
from datetime import date, datetime
from pandas import DataFrame

"""
Данный набор тестов покрывает:
1) Работа с путями.
    а) Корректность путей для Luigi task из скрипта luigi_task передаваймых аргументами.
    {<--ExternalData.ExternalData-external-data-path>,
    <--ExtractTask.ExtractTask-extract-data-path>,
    <--TransformTask.TransformTask-date-path-part>,
    <--LoadTask.LoadTask-load-data-path>}
        - Существует ли путь передоваймый в аргументе.
        - Проверка что путь передоваймый в аргументе не содержет '/', в конце.
        - Наличие не пустой строки в аргументе, передоваймом таске.
        - Проверка что путь из аргумента это строка.
        # последние два пункта проверяет сам Luigi дополнительно.
    б) Корректность путей для ExternalData task.
        - Все проверки путей для тасок выше.
        - В правильном ли формате указанны пути до партиций.
    в) Корректность маски для пути TransformTask, передоваймой в аргументе.
    {<--TransformTask.TransformTask-date-path-part>}
        - Проверка что эта маска является датой, в формате '%Y/%m/%d'.
        # Luigi так же проверяет это сам.
2) Работа с масками расширения файлов передаваймыми в аргументах.
{<--ExtractTask.ExtractTask-extract-file-mask>,
<--TransformTask.TransformTask-transform-file-mask>,
<--LoadTask.LoadTask-load-file-mask>}
    а) Проверка что маски из аргументов являются строками.
    б) Проверка что строки маски не пустые.
    б) Проверка содержимого строк на корректрые расширения.
        - Json
        - parquet
        - CSV
3) Работа с файлами и данными.
    а) Файл для записи был создан.
    б) К записи готовится не пустой датафрейм.
    
Тесты запускаются в процессе работы luigi пайплайна и валят исполнение в случии ошибок, с выводом информации о них  
в терминал.
"""


def error_warp(funk):
    """Декоратор ошибок:"""
    print('====================== error: ======================')
    print('')
    print('')
    funk()
    print('')
    print('')
    print('====================================================')
    exit(1)


def test_external_task_path(arg):
    """
    Тест передовайномго в параметре пути:
    Прверяет наличие ./YYYY/MM/DD/ в корневом каталоге,
    переданном в аргументе.
    """
    def incorrect_paths_in_dir_error():
        print('Root path <' + arg + '>:')
        print("Have no data dirs in format '%Y/%m/%d'.")
        print('Example ./YYYY/MM/DD/.')
    is_it_date_in_path = []
    for dirs, folder, files in walk(arg):
        search_for_an_interesting_path = dirs.replace(arg, '')
        is_it_date_in_path.append(search_for_an_interesting_path)
    ok_count = 0
    for element in is_it_date_in_path:
        if '/' in element:
            check = element.split('/')
            if len(check) >= 4:
                is_it_date = f"{check[-3]}/{check[-2]}/{check[-1]}"
                try:
                    datetime.strptime(is_it_date, '%Y/%m/%d').date()
                    ok_count = ok_count + 1
                except ValueError:
                    continue
    if ok_count == 0:
        error_warp(incorrect_paths_in_dir_error)
    return ok_count


def test_path_mask_type_for_date(arg):
    """
    Тест передовайномго в параметре пути:
    1) Наличие не пустой строки в аргументе.
    2) Проверка пути на существование.
    3) Проверка последнего символа в пути.
    """
    def path_das_not_exist_error():
        print('Переданный в качестве параметра Luigi таски путь <' + arg + '> не существует.')

    def path_symbol_error():
        print("В передоваймом Luigi таске аргументе корневом пути, не должно быть символа '/' в конце.")
        print('Таска сама добавит нужный символ в процессе работы.')
        print('Текущее значение аргумента пути: <' + arg + '>.')

    def path_incorrect_format_error():
        print('В параметре для корневого каталог Luigi таски ничего не переданно.')
        print('Либо переданна не строка.')
    if len(arg) != 0 and type(arg) is str:
        if not path.exists(arg):
            error_warp(path_das_not_exist_error)
        if arg[-1] == '/':
            error_warp(path_symbol_error)
    else:
        error_warp(path_incorrect_format_error)


def test_transform_task_time_mask(arg):
    """Проверяет маску date_path_part на соответсвие дате в формате '%Y/%m/%d'."""
    def mask_error():
        print('В параметре маски времени для пути TransformTask Luigi таски ничего не переданно.')
        print("Или в ней не дата в формате '%Y/%m/%d'.")
    if type(arg) is not date:
        error_warp(mask_error)
    try:
        check = arg.strftime('%Y/%m/%d')
        datetime.strptime(check, '%Y/%m/%d').date()
    except ValueError:
        error_warp(mask_error)


def test_file_mask_arguments(arg):
    """
    Проверяет что маски файлов в аргументах пайплайна это строки
    соответсвующие требуемым форматам.
    """
    def is_not_str_error():
        print('Аргумент полученный в <' + arg + '> не является строкой.')
        print('Или передан пустой аргумент.')

    def type_of_arg_is_not_correct():
        print('Переданный <' + arg + '> аргумент не относится к типам:')
        print('.json')
        print('.csv')
        print('.parquet')
        print("Или не является расширением файла, в формате '.type'.")
    if type(arg) is str and len(arg) != 0:
        if arg != '.json' and arg != '.csv' and arg != '.parquet':
            error_warp(type_of_arg_is_not_correct)
    else:
        error_warp(is_not_str_error)


def test_output_df(arg):
    """Проверяет что готовищийся к записи в файл pandas DF не пустой."""
    def df_is_empty():
        print('К записи был направлен пустой pandas DF.')
    if len(arg.index) == 0:
        error_warp(df_is_empty)


def test_output_file_exist(arg):
    """Проверяет создался ли файл с результатом работы Luigi таски."""
    def file_exist_error():
        print('Файл с результатом не был создан. Путь:')
        print(arg)
    try:
        path.exists(arg)
    except OSError:
        error_warp(file_exist_error)
