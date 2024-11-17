from functions import *
import time
import requests
import json
import urllib


def try_download_seller_products_from_ozon():
    url_otchet = "https://api-seller.ozon.ru/v1/report/products/create"
    data_otchet = {
        "language": "DEFAULT",
        "offer_id": [],
        "search": "",
        "sku": [],
        "visibility": "ALL"
    }
    headers = {'Client-Id': '1642603', 'Api-Key': 'acae0dbe-e77e-4a21-ae64-6d55ac3476d8'}
    response_otchet = requests.post(url_otchet, data=json.dumps(data_otchet), headers=headers).json()
    answer = response_otchet.get("result")

    if answer is None:
        print('ВНИМАНИЕ!!! Произошла ошибка при ГЕНЕРАЦИИ товары.цсв, зовите программиста или выкачайте файл руками')
        print(response_otchet['message'])
    else:
        url_download = 'https://api-seller.ozon.ru/v1/report/info'
        data_download = {'code': answer['code']}
        tries = 0
        while 1:
            response_download = requests.post(url_download, data=json.dumps(data_download), headers=headers).json()
            answer_download = response_download.get('result')
            if answer_download is None:
                print(
                    'ВНИМАНИЕ!!! Произошла ошибка при ПОЛУЧЕНИИ ССЫЛКИ на товары.цсв, зовите программиста или '
                    'выкачайте файл руками')
                print(response_download['message'])
            elif answer_download['status'] == 'success':
                tovari_csv_url = answer_download['file']
                try:
                    urllib.request.urlretrieve(tovari_csv_url, "seller_products.csv")
                    print('Товары.цсв скачан успешно!')
                except urllib.error.HTTPError as e:
                    print(
                        'ВНИМАНИЕ!!! Произошла ошибка при СКАЧИВАНИИ товары.цсв, зовите программиста или выкачайте '
                        'файл руками')
                    print(e)
                break
            elif answer_download['status'] == 'waiting' or answer_download['status'] == 'processing':
                if tries == 10:
                    print('ВНИМАНИЕ!!! СОЗДАНИЕ товары.цсв внутри озона не получилось за 10 сек'
                          ', зовите программиста или выкачайте файл руками')
                    break
                time.sleep(2)
                tries += 1
                print('Попытка скачать товары.цсв, ждем ответ озона №' + str(tries) + '.' * tries)
            else:
                print(
                    "Непредусмотренная ошибка при получении товары.цсв от озон. Пробуйте перезапустить программу или "
                    "зовите программиста")


def parse_our_fbs_stock():
    our_fbs_stock = pd.read_excel('Текущие остатки.xlsx')
    our_fbs_stock = our_fbs_stock.fillna('0')
    our_fbs_stock = our_fbs_stock[our_fbs_stock['Количество'] != 0]
    return our_fbs_stock


def parse_seller_products(path):
    seller_products = pd.read_csv(path, sep=';')
    seller_products['Артикул'] = seller_products.apply(lambda row: row['Артикул'][1:], axis=1)
    seller_products['Остаток'] = (seller_products['Доступно к продаже по схеме FBO, шт.']
                                  + seller_products['Доступно к продаже по схеме FBS, шт.'])

    seller_products.rename(columns={'Barcode': 'Штрих код'}, inplace=True)
    seller_products['Штрих код'] = seller_products['Штрих код'].fillna(0)
    seller_products['Штрих код'] = seller_products['Штрих код'].astype("string")
    seller_products.sort_values(by='Артикул', inplace=True)
    seller_products['Штрих код'] = seller_products['Штрих код'].apply(lambda x: get_text_before_1st_minus_sign(x))
    return seller_products


def process_alpaka(alpaka_prefixed, alpaka_word, seller_products, our_fbs_stock):
    alpaka_df = pd.read_excel('prices/' + alpaka_prefixed[0])

    alpaka_df = alpaka_df.drop(alpaka_df.columns[[0, 2, 3, 6, 7, 9, 10, 11]], axis=1)
    alpaka_df.columns = alpaka_df.loc[3].values.flatten().tolist()
    alpaka_df = alpaka_df.drop([0, 1, 2, 3], axis=0)
    alpaka_df = alpaka_df[alpaka_df['Штрихкод'].notna()]

    alpaka_df['Наличие'] = alpaka_df['Наличие'].astype(int)
    alpaka_df['Наличие'] = alpaka_df['Наличие'].apply(lambda x: fill_alpaka_ostatok(x))
    alpaka_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)
    print('Колонки:'+str(alpaka_df.columns.values))
    alpaka_df = alpaka_df[alpaka_df['Наименование'].str.startswith(("Advance", 'NT', 'RIO', 'КОТУ', 'Нап д/туал'))
                          | alpaka_df['Наименование'].str.contains(
        'GIMCAT|GIMСАT|Pucek|Песок для купания|Cliny')]  # "СА" русские во 2м варианте
    alpaka_df_merged = alpaka_df.merge(seller_products, on='Штрих код', how='left')
    alpaka_df_new = alpaka_df_merged[alpaka_df_merged['Артикул'].isna()]
    alpaka_df_new.to_excel('new kartochki/' + alpaka_word + '.xlsx', index=False)
    info_new_katrochki(len(alpaka_df_new), alpaka_word)
    alpaka_df_merged = alpaka_df.merge(seller_products, on='Штрих код')

    alpaka_stop_list = pd.read_excel('stop list/' + alpaka_word + '.xlsx', converters={'Штрих код': str})
    alpaka_stop_list = alpaka_stop_list[alpaka_stop_list['Продаем(да,нет)'] == 'нет']
    alpaka_df_merged['Наличие'] = alpaka_df_merged['Наличие'] * (
        ~alpaka_df_merged['Штрих код'].isin(alpaka_stop_list['Штрих код'])).astype(int)

    alpaka_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
    alpaka_stock_template['Артикул'] = alpaka_df_merged['Артикул']
    alpaka_stock_template['Количество'] = alpaka_df_merged['Наличие']

    alpaka_stock_template['Количество'] = alpaka_stock_template.apply(lambda row: add_our_fbs_stock(row, our_fbs_stock),
                                                                      axis=1)
    alpaka_stock_template['Имя (необязательно)'] = alpaka_df_merged['Штрих код']
    alpaka_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
    alpaka_stock_template['Цена с НДС'] = alpaka_df_merged['Цена с НДС']
    alpaka_stock_template['Название склада'] = sklad_name
    alpaka_stock_template.to_excel('result/' + alpaka_word + '.xlsx', index=False)
    info_price_proccessed(alpaka_word)


def process_spk(spk_prefixed, spk_word, seller_products, our_fbs_stock):
    spk_df = pd.read_excel('prices/' + spk_prefixed[0])
    spk_df = spk_df.drop(spk_df.columns[[1, 3, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 18]], axis=1)
    spk_df.columns = spk_df.loc[11].values.flatten().tolist()
    spk_df = spk_df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], axis=0)
    spk_df = spk_df[spk_df['Штрихкод'].notna()]
    spk_df['Штрихкод'] = spk_df['Штрихкод'].apply(lambda x: x.replace('шк', ''))
    spk_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)
    print('Колонки:' + str(spk_df.columns.values))
    spk_df.columns = spk_df.columns.fillna('Цена с НДС')
    spk_df = spk_df[spk_df['Номенклатура'].str.startswith(("ACANA", 'ORIJEN', 'ON '))]

    spk_seller_products = seller_products[seller_products['Артикул'].str.startswith('ВВП')]
    spk_df_merged = spk_df.merge(seller_products, on='Штрих код', how='left')
    spk_df_new = spk_df_merged[spk_df_merged['Артикул_y'].isna()]
    info_new_katrochki(len(spk_df_new), spk_word)
    spk_df_merged = spk_seller_products.merge(spk_df, on='Штрих код', how='left')
    spk_df_new.to_excel('new kartochki/' + spk_word + '.xlsx', index=False)

    spk_df_merged['Остаток'] = spk_df_merged.apply(lambda row: fill_null_with_0_else_randint(row['Номенклатура']),
                                                   axis=1)

    spk_stop_list = pd.read_excel('stop list/' + spk_word + '.xlsx', converters={'Штрих код': str})
    spk_stop_list = spk_stop_list[spk_stop_list['Продаем(да,нет)'] == 'нет']
    spk_df_merged = spk_df_merged[~spk_df_merged['Штрих код'].isin(spk_stop_list['Штрих код'])]
    spk_df_merged['Остаток'] = spk_df_merged['Остаток'] * (
        ~spk_df_merged['Штрих код'].isin(spk_stop_list['Штрих код'])).astype(int)

    spk_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
    spk_stock_template['Артикул'] = spk_df_merged['Артикул_x']
    spk_stock_template['Количество'] = spk_df_merged['Остаток']
    spk_stock_template['Количество'] = spk_stock_template.apply(lambda row: add_our_fbs_stock(row, our_fbs_stock),
                                                                axis=1)
    spk_stock_template['Имя (необязательно)'] = spk_df_merged['Штрих код']
    spk_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
    spk_stock_template['Цена с НДС'] = spk_df_merged['Цена с НДС']
    spk_stock_template['Название склада'] = sklad_name
    spk_stock_template.to_excel('result/' + spk_word + '.xlsx', index=False)
    info_price_proccessed(spk_word)


def process_trbt(trbt_prefixed, trbt_word, seller_products, our_fbs_stock):
    trbt_df = pd.read_excel('prices/' + trbt_prefixed[0])
    trbt_df = trbt_df.drop(trbt_df.columns[[0, 1, 2, 4, 5, 6, 7, 10, 12, 13]], axis=1)
    trbt_df = trbt_df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], axis=0)
    trbt_df.rename(columns={'Unnamed: 3': 'Номенклатура', 'Unnamed: 9': 'Штрих код', 'Unnamed: 8': 'Свободный остаток',
                            'Unnamed: 11': 'Цена с НДС'}, inplace=True)
    trbt_df = trbt_df[~trbt_df['Штрих код'].isna()]
    trbt_df['Штрих код'] = trbt_df['Штрих код'].astype("string")
    print('Колонки:' + str(trbt_df.columns.values))

    trbt_seller_products = seller_products[seller_products['Артикул'].str.startswith('ТРБТ')]
    trbt_df_merged = trbt_df.merge(seller_products, on='Штрих код', how='left')
    trbt_df_new = trbt_df_merged[trbt_df_merged['Артикул'].isna()]
    trbt_df_new.to_excel('new kartochki/' + trbt_word + '.xlsx', index=False)
    info_new_katrochki(len(trbt_df_new), trbt_word)
    trbt_df_merged = trbt_seller_products.merge(trbt_df, on='Штрих код', how='left')

    trbt_stop_list = pd.read_excel('stop list/' + trbt_word + '.xlsx', converters={'Штрих код': str})
    trbt_stop_list = trbt_stop_list[trbt_stop_list['Продаем(да,нет)'] == 'нет']

    trbt_df_merged['Свободный остаток'] = trbt_df_merged['Свободный остаток'].apply(
        lambda x: fill_null_with_0_else_randint(x))
    trbt_df_merged = trbt_df_merged[~trbt_df_merged['Штрих код'].isin(trbt_stop_list['Штрих код'])]
    trbt_df_merged['Свободный остаток'] = trbt_df_merged['Свободный остаток'] * (
        ~trbt_df_merged['Штрих код'].isin(trbt_stop_list['Штрих код'])).astype(int)

    trbt_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
    trbt_stock_template['Артикул'] = trbt_df_merged['Артикул']
    trbt_stock_template['Количество'] = trbt_df_merged['Свободный остаток']
    trbt_stock_template['Количество'] = trbt_stock_template.apply(lambda row: add_our_fbs_stock(row, our_fbs_stock),
                                                                  axis=1)
    trbt_stock_template['Имя (необязательно)'] = trbt_df_merged['Штрих код']
    trbt_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
    trbt_stock_template['Название склада'] = sklad_name
    trbt_stock_template['Цена с НДС'] = trbt_df_merged['Цена с НДС']
    trbt_stock_template.to_excel('result/' + trbt_word + '.xlsx', index=False)
    info_price_proccessed(trbt_word)


def process_zoom(zoom_prefixed, zoom_word, seller_products, our_fbs_stock):
    zoom_df = pd.read_excel('prices/' + zoom_prefixed[0])
    zoom_df = zoom_df.drop(zoom_df.columns[[0, 4, 5, 6, 8]], axis=1)
    zoom_df.columns = zoom_df.loc[7].values.flatten().tolist()
    zoom_df = zoom_df.drop([0, 1, 2, 3, 4, 5, 6, 7], axis=0)
    zoom_df['Номенклатура.Основной штрихкод'] = zoom_df['Номенклатура.Основной штрихкод'].shift(periods=[-1])
    zoom_df['ЗМ c НДС'] = zoom_df['ЗМ c НДС'].shift(periods=[-1])
    zoom_df = zoom_df.drop_duplicates(subset=['Номенклатура.Основной штрихкод'])
    zoom_df = zoom_df[zoom_df['Номенклатура.Основной штрихкод'].notna()]
    zoom_df['Номенклатура.Основной штрихкод'] = zoom_df['Номенклатура.Основной штрихкод'].astype(str)
    zoom_df = zoom_df[zoom_df['Номенклатура.Основной штрихкод'].str.contains(pat="[0-9]{13}", regex=True, na=True)]
    zoom_df.rename(columns={'Номенклатура.Основной штрихкод': 'Штрих код'}, inplace=True)
    zoom_df['Штрих код'] = zoom_df['Штрих код'].astype("string")
    print('Колонки:' + str(zoom_df.columns.values))

    zoom_df_merged = zoom_df.merge(seller_products, on='Штрих код', how='left')
    zoom_df_new = zoom_df_merged[zoom_df_merged['Артикул'].isna()]
    zoom_df_new.to_excel('new kartochki/' + zoom_word + '.xlsx', index=False)
    info_new_katrochki(len(zoom_df_new), zoom_word)
    zoom_seller_products = seller_products[seller_products['Артикул'].str.startswith('ЗООМ')]
    zoom_df_merged = zoom_seller_products.merge(zoom_df, on='Штрих код', how='left')

    zoom_stop_list = pd.read_excel('stop list/' + zoom_word + '.xlsx', converters={'Штрих код': str})
    zoom_stop_list = zoom_stop_list[zoom_stop_list['Продаем(да,нет)'] == 'нет']

    zoom_df_merged['Остаток_x'] = (zoom_df_merged['Ценовая группа/ Номенклатура/ Характеристика номенклатуры']
                                   .apply(lambda x: fill_null_with_0_else_randint(x)))
    zoom_df_merged['Остаток_x'] = zoom_df_merged['Остаток_x'] * (
        ~zoom_df_merged['Штрих код'].isin(zoom_stop_list['Штрих код'])).astype(int)

    zoom_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
    zoom_stock_template['Артикул'] = zoom_df_merged['Артикул']
    zoom_stock_template['Количество'] = zoom_df_merged['Остаток_x']
    zoom_stock_template['Количество'] = zoom_stock_template.apply(lambda row: add_our_fbs_stock(row, our_fbs_stock),
                                                                  axis=1)
    zoom_stock_template['Имя (необязательно)'] = zoom_df_merged['Штрих код']
    zoom_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
    zoom_stock_template['Название склада'] = sklad_name
    zoom_stock_template['Цена с НДС'] = zoom_df_merged['ЗМ c НДС']
    zoom_stock_template.to_excel('result/' + zoom_word + '.xlsx', index=False)
    info_price_proccessed(zoom_word)


def process_tian(tian_prefixed, tian_word, seller_products, our_fbs_stock):
    tian_df = pd.read_excel('prices/' + tian_prefixed[0])
    tian_df = tian_df.drop(tian_df.columns[[0, 2, 3, 5, 7, 8, 9, 10]], axis=1)
    tian_df.columns = tian_df.loc[2].values.flatten().tolist()
    tian_df = tian_df.drop([0, 1, 2, 3], axis=0)

    tian_df = rename_barcode_col(tian_df)
    tian_df = tian_df[~tian_df['Штрих код'].isna()]
    tian_df['Штрих код'] = tian_df['Штрих код'].astype("string")
    print('Колонки:' + str(tian_df.columns.values))
    tian_df = tian_df[tian_df['Наименование товаров'].str.contains('AMBROSIA')]

    tian_seller_products = seller_products[seller_products['Артикул'].str.startswith('ТИАН')]
    tian_df_merged = tian_df.merge(tian_seller_products, on='Штрих код', how='left')
    tian_df_new = tian_df_merged[tian_df_merged['Артикул'].isna()]
    tian_df_new.to_excel('new kartochki/' + tian_word + '.xlsx', index=False)
    info_new_katrochki(len(tian_df_new), tian_word)
    tian_df_merged = tian_seller_products.merge(tian_df, on='Штрих код', how='left')

    tian_stop_list = pd.read_excel('stop list/' + tian_word + '.xlsx', converters={'Штрих код': str})
    tian_stop_list = tian_stop_list[tian_stop_list['Продаем(да,нет)'] == 'нет']
    tian_df_merged = tian_df_merged[~tian_df_merged['Штрих код'].isin(tian_stop_list['Штрих код'])]

    tian_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
    tian_stock_template['Артикул'] = tian_df_merged['Артикул']
    tian_stock_template['Количество'] = tian_df_merged['Остаток'].apply(lambda x: fill_null_with_0_else_randint(x))
    tian_stock_template['Количество'] = tian_stock_template.apply(lambda row: add_our_fbs_stock(row, our_fbs_stock),
                                                                  axis=1)
    tian_stock_template['Имя (необязательно)'] = tian_df_merged['Штрих код']
    tian_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
    tian_stock_template['Название склада'] = sklad_name
    tian_stock_template['Цена с НДС'] = tian_df_merged['Цена  с НДС 20%']
    tian_stock_template.to_excel('result/' + tian_word + '.xlsx', index=False)
    info_price_proccessed(tian_word)


def process_anmd(anmd_prefixed, anmd_word, seller_products, our_fbs_stock):
    anmd_df = pd.read_excel('prices/' + anmd_prefixed[0])
    anmd_df = anmd_df.drop(anmd_df.columns[[0, 3, 4, 7, 8, 9]], axis=1)
    anmd_df.columns = anmd_df.loc[1].values.flatten().tolist()
    anmd_df = anmd_df.drop([0, 1, 2, 3], axis=0)
    anmd_df = anmd_df[anmd_df['Штрих-код'].notna()]
    anmd_df.rename(columns={'Артикул': 'Штрих код', 'Штрих-код': 'Полный ШК'}, inplace=True)
    anmd_df['Штрих код'] = anmd_df['Штрих код'].astype("string")
    anmd_df['Полный ШК'] = anmd_df['Полный ШК'].astype("string")
    print('Колонки:' + str(anmd_df.columns.values))

    anmd_seller_products = seller_products[seller_products['Артикул'].str.startswith('ANMD')]
    anmd_df_merged = anmd_df.merge(seller_products, on='Штрих код', how='left')
    anmd_df_new = anmd_df_merged[anmd_df_merged['Артикул'].isna()]
    anmd_df_merged = anmd_seller_products.merge(anmd_df, on='Штрих код', how='left')
    anmd_df_new.to_excel('new kartochki/' + anmd_word + '.xlsx', index=False)
    info_new_katrochki(len(anmd_df_new), anmd_word)

    anmd_df_merged['Остаток'] = anmd_df_merged.apply(lambda row: fill_null_with_0_else_randint(row['Полный ШК']),
                                                     axis=1)

    anmd_stop_list = pd.read_excel('stop list/' + anmd_word + '.xlsx', converters={'Штрих код': str})
    anmd_stop_list = anmd_stop_list[anmd_stop_list['Продаем(да,нет)'] == 'нет']
    anmd_df_merged = anmd_df_merged[~anmd_df_merged['Штрих код'].isin(anmd_stop_list['Штрих код'])]
    anmd_df_merged['Остаток'] = anmd_df_merged['Остаток'] * (
        ~anmd_df_merged['Штрих код'].isin(anmd_stop_list['Штрих код'])).astype(int)

    anmd_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
    anmd_stock_template['Артикул'] = anmd_df_merged['Артикул']
    anmd_stock_template['Количество'] = anmd_df_merged['Остаток']
    anmd_stock_template['Количество'] = anmd_stock_template.apply(lambda row: add_our_fbs_stock(row, our_fbs_stock),
                                                                  axis=1)
    anmd_stock_template['Имя (необязательно)'] = anmd_df_merged['Штрих код']
    anmd_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
    anmd_stock_template['Цена с НДС'] = anmd_df_merged['Цена по предоплате (без НДС), руб'] * 1.2
    anmd_stock_template['Название склада'] = sklad_name
    anmd_stock_template.to_excel('result/' + anmd_word + '.xlsx', index=False)
    info_price_proccessed(anmd_word)


def process_hntr(hntr_prefixed, hntr_word, seller_products, our_fbs_stock):
    hntr_df = pd.read_excel('prices/' + hntr_prefixed[0])
    hntr_df = hntr_df.drop(hntr_df.columns[[0, 1, 2, 4, 5, 6, 7, 10, 11, 12]], axis=1)
    hntr_df.columns = hntr_df.loc[1].values.flatten().tolist()
    hntr_df = hntr_df.drop([0, 1], axis=0)
    hntr_df = rename_barcode_col(hntr_df)
    hntr_df = hntr_df[~hntr_df['Штрих код'].isna()]
    hntr_df['Штрих код'] = hntr_df['Штрих код'].astype("string")
    print('Колонки:' + str(hntr_df.columns.values))
    hntr_seller_products = seller_products[seller_products['Артикул'].str.startswith('ХНТР')]
    hntr_df_merged = hntr_df.merge(hntr_seller_products, on='Штрих код', how='left')
    hntr_df_new = hntr_df_merged[hntr_df_merged['Артикул'].isna()]
    hntr_df_new.to_excel('new kartochki/' + hntr_word + '.xlsx', index=False)
    info_new_katrochki(len(hntr_df_new), hntr_word)
    hntr_df_merged = hntr_seller_products.merge(hntr_df, on='Штрих код', how='left')
    hntr_stop_list = pd.read_excel('stop list/' + hntr_word + '.xlsx', converters={'Штрих код': str})
    hntr_stop_list = hntr_stop_list[hntr_stop_list['Продаем(да,нет)'] == 'нет']
    hntr_df_merged = hntr_df_merged[~hntr_df_merged['Штрих код'].isin(hntr_stop_list['Штрих код'])]
    hntr_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
    hntr_stock_template['Артикул'] = hntr_df_merged['Артикул']
    hntr_stock_template['Количество'] = hntr_df_merged['остаток'].apply(lambda x: fill_null_with_0_else_randint(x))
    hntr_stock_template['Количество'] = hntr_stock_template.apply(lambda row: add_our_fbs_stock(row, our_fbs_stock),
                                                                  axis=1)
    hntr_stock_template['Имя (необязательно)'] = hntr_df_merged['Штрих код']
    hntr_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
    hntr_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
    hntr_stock_template['Цена с НДС'] = hntr_df_merged['Цена со скидкой с НДС']
    hntr_stock_template.to_excel('result/' + hntr_word + '.xlsx', index=False)
    info_price_proccessed(hntr_word)