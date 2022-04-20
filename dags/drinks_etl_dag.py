import logging
import shutil
import time
from datetime import datetime
from pprint import pprint
import io

import cv2
import xml.etree.ElementTree as ET
import os
import ngtpy
import pickle
import glob
import psycopg2
import requests
import urllib.parse
import fitz
from PIL import Image
import magic
import base64
import zipfile

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator


log = logging.getLogger(__name__)



DRINKS_AIRFLOW_DAG_VERSION = 30



DRINKS_DATA_DIR = '/opt/airflow/resources/drinks/drinks_data'
ASSETS_FRAP_DIR = '/opt/airflow/resources/drinks/assets_frap'

GET_FROM_CVAT_RETRY_COUNT = 10


def telegram(message):
    print(message)
    access_token = Variable.get('drinks_telegram_access_token')
    chat_id = Variable.get('drinks_telegram_chat_id_ci_ml') 
    
    if 'dev' in Variable.get('drinks_profiles'):
        print(f'Profiles: {Variable.get("drinks_profiles")}')
        chat_id = Variable.get('drinks_telegram_chat_id_testd77') 
            
    send_url = 'https://api.telegram.org/bot' + str(access_token) + '/sendMessage?' + \
                    'chat_id=' + str(chat_id) + \
                    '&parse_mode=Markdown' + \
                    '&text=' + message

    response = requests.get(send_url)
    print(f'Telegram responce: {response}')
    print(f'Telegram responce: {response.text}')
    



with DAG(
    dag_id=f'drinks_etl_dag',
    schedule_interval='0 9 * * *',
    start_date=datetime(2022, 4, 10),
    catchup=False,
    tags=['drinks'],
    max_active_runs=1
) as dag:

    
    
    @task(task_id='start_notification')
    def start_notification(**kwargs):
        # pprint(kwargs)
        print('start_notification')
        # Самый первый last_id = 970
        last_id = Variable.get("drinks_last_id")
        print(f'drinks_last_id: {last_id}')
        
        telegram(f'*Airflow Drinks ETL DAG*\n* start notification*\n```  version:{DRINKS_AIRFLOW_DAG_VERSION:>6}\n  last id:{last_id:>6}```')
        
        
        
        
        
    start_notification_task = start_notification()
    
    
    
        
        
    @task(task_id='image_to_cvat')
    def image_to_cvat():
        
        print('image_to_cvat')
        print(f'Profiles: {Variable.get("drinks_profiles")}')
        
        host = Variable.get('drinks_db_host')
        port = Variable.get('drinks_db_port')
        db_name = Variable.get('drinks_db_name')
        user = Variable.get('drinks_db_user')
        password = Variable.get('drinks_db_password')
        last_id = Variable.get("drinks_last_id")
        sql_select_images = Variable.get("drinks_sql_select_images")
        
        if 'dev' in Variable.get('drinks_profiles'):
            sql_select_images = Variable.get('drinks_sql_select_images_with_limit') 
        
        
        print(f'last_id: {last_id}')
        print(f'sql_select_image: {sql_select_images}')
        
        # Получаем новую алкогольную продукцию
        conn = psycopg2.connect(host=host, port=port, database=db_name, user=user, password=password)
        cur = conn.cursor()
        cur.execute(sql_select_images, (last_id,))
        query_results = cur.fetchall()
        conn.close()
        
        query_results_length = len(query_results)
        print(f'Получено новой продукции: {query_results_length}')
            
        # Количество картинок, которые вытащим из полученых PDF
        jpg_count = 0
        
        files_for_cvat = []

        
        if query_results_length > 0:
            
            timestr = time.strftime('%Y%m%d-%H%M%S')
            relative_directory = f'cvat/{timestr}/img'
            absolute_directory = f'{DRINKS_DATA_DIR}/{relative_directory}'
            if not os.path.exists(absolute_directory):
                os.makedirs(absolute_directory)



            for i, row in enumerate(query_results, start=1):

                print(f'Строка из БД: {row}')

                claim_id = row[0]
                # class for index model
                container_id = row[1]
                frap_pdf = row[2]
                
                
                container_id_dir = f'{ASSETS_FRAP_DIR}/{container_id}'
                            

                # Нужно обновлять последний обработанный идентификатор
                if i == query_results_length:
                    last_id = claim_id


                # with open(f'{directory}/{row[0]}.pdf', 'wb') as file:
                #     file.write(row[8])

                # Необходимо перевести тип memoryview в массив байт
                frap_pdf_bytes = frap_pdf.tobytes()

                file_type = magic.from_buffer(frap_pdf_bytes, mime=True)
                print(f'Тип файла: {file_type}')
                if 'pdf' not in file_type:
                    print('WARN Не обрабатывается')
                    continue

                pdf_file = fitz.open(stream=frap_pdf_bytes, filetype='pdf')

                # iterate over pdf pages
                for page_index in range(len(pdf_file)):
                    # get the page itself
                    page = pdf_file[page_index]
                    image_list = page.get_images()

                    # printing number of images found in this page
                    if image_list:
                        print(f"Найдено {len(image_list)} картинок на странице {page_index}")
                    else:
                        print("Не найдено картинок на странице", page_index)

                    if len(image_list) > 0 and not os.path.exists(container_id_dir):
                        os.makedirs(container_id_dir)
                        
                        
                    for image_index, img in enumerate(page.get_images(), start=1):

                        xref = img[0]
                        base_image = pdf_file.extract_image(xref)
                        image_bytes = base_image["image"]
                        image_ext = base_image["ext"]
                        image = Image.open(io.BytesIO(image_bytes))

                        # frap_id_directory = f'{directory}/{frap_id}'
                        # if not os.path.exists(frap_id_directory):
                        #     os.makedirs(frap_id_directory)

                        with open(f"{absolute_directory}/{container_id}_{page_index+1}_{image_index}.{image_ext}", "wb") as jpg:
                            image.save(jpg)
                        print(f'Сохранена картинка {absolute_directory}/{container_id}_{page_index+1}_{image_index}.{image_ext}')
                        files_for_cvat.append(f"{relative_directory}/{container_id}_{page_index+1}_{image_index}.{image_ext}")
                        
                        
                        
                        with open(f"{container_id_dir}/{container_id}_{page_index+1}_{image_index}.{image_ext}", "wb") as jpg:
                            image.save(jpg)
                        print(f'Сохранена картинка для assets_frap {container_id_dir}/{container_id}_{page_index+1}_{image_index}.{image_ext}')




            cur.close()


            if len(files_for_cvat) > 0:


                org = Variable.get('drinks_cvat_org')
                user = Variable.get('drinks_cvat_user')
                password = Variable.get('drinks_cvat_password')
                auth_token = base64.b64encode(f'{user}:{password}'.encode()).decode()
                headers = {'authorization': f'Basic {auth_token}'}


                create_task_json = {
                  "name": timestr,
                  "project_id": int(Variable.get('drinks_cvat_project_id'))
                }
                print('create_task_json', create_task_json)

                cvat_address = Variable.get('drinks_cvat_address')
                url = f'{cvat_address}/api/tasks?org={org}'
                response = requests.post(url, json=create_task_json, headers=headers)
                print(response)
                print(response.json())
                new_task_id = response.json()['id']
                print(new_task_id)


                data = {}
                data['image_quality'] = 100
                data['server_files'] =  files_for_cvat

                print(data)

                url = f'{cvat_address}/api/tasks/{new_task_id}/data'
                response = requests.post(url, json=data, headers=headers)
                print(response)
                response.json()


            # Нужно обновлять последний обработанный идентификатор
            Variable.set('drinks_last_id', last_id)
        
        
        telegram(f'*Airflow Drinks ETL DAG*\n* image to cvat*\n```  pdf:{query_results_length:>10}\n  jpeg:{len(files_for_cvat):>9}\n  last id:{last_id:>6}```')
        
     
    
    image_to_cvat_task = image_to_cvat()
    
            
            
    
    
    
    
    def get_cvat_tasks(address, status, headers, org, page=1, pageSize==100000, sort='id'):
        filterParam = urllib.parse.quote(f'{{"==":[{{"var":"status"}},"{status}"]}}')
        url = f'{address}/api/tasks?org={org}&page={page}&page_size={pageSize}&sort={sort}&filter={filterParam}'
        response = requests.get(url, headers=headers)
        print(response)
        print(response.json())
        tasks = {r['id']: r['name'] for r in response.json()['results']}
        print(f'{status} tasks: {tasks}')
        return tasks
    
    
    
    # @task(task_id='cvat_exported_crop_lable')
    def cvat_exported_crop_lable(**kwargs):
        print('cvat_exported_crop_lable')
        
        cvat_address = Variable.get('drinks_cvat_address')
        
        org = Variable.get('drinks_cvat_org')
        page = Variable.get('drinks_cvat_tasks_page', 1)
        pageSize = Variable.get('drinks_cvat_tasks_page_size', 100000)
        sort = Variable.get('drinks_cvat_tasks_sort', 'id')
        user = Variable.get('drinks_cvat_user')
        password = Variable.get('drinks_cvat_password')
        auth_token = base64.b64encode(f'{user}:{password}'.encode()).decode()
        headers = {'authorization': f'Basic {auth_token}'}
        
        
        cvat_tasks_annotation = get_cvat_tasks(cvat_address, 'annotation', headers, org, page, pageSize, sort)
        cvat_tasks_validation = get_cvat_tasks(cvat_address, 'validation', headers, org, page, pageSize, sort)
        cvat_tasks_complete = get_cvat_tasks(cvat_address, 'complete', headers, org, page, pageSize, sort)
        
        # Если есть папка labels и она не пустая, то считаем, что уже обработали
        labels_processed = glob.glob(f'{DRINKS_DATA_DIR}/cvat/*/labels/*')
        tasks_processed = {label_path.split(os.sep)[-3] for label_path in labels_processed}
        print(f'Уже обработанные задачи: {tasks_processed}')

        # Сколько сечас обработается cvat тасков
        tasks_processed_total_counter = 0
        # Сколько сейчас придет всего картинок от cvat (и размеченных и нет, по каким-то причинам)
        dag_task_images_total_counter = 0
        # Сколько сейчас будет обработано размеченных картинок
        dag_task_labeled_images_total_counter = 0
        # Сколько сейчас получится этикеток из всех картинок
        dag_task_labeles_total_counter = 0
        
        
        for task_id, task_name in cvat_tasks_complete.items():
            
            if task_name in tasks_processed:
                print(f'Задача task_id: {task_id}, task_name: {task_name} уже обработанна')
                continue
    
            params = {
                'format': 'CVAT for images 1.1',
                'action': 'download'
            }
            url = f'{cvat_address}/api/v1/tasks/{task_id}/annotations'

            retry_index = 0
            while retry_index < GET_FROM_CVAT_RETRY_COUNT:
                retry_index += 1
                print(f'Попытка номер {retry_index} экспортировать разметку для задачи id: {task_id}, name: {task_name}')
                
                response = requests.get(url, headers=headers, params=params)
                print('content-type', response.headers.get('content-type'))
                print('status_code', response.status_code)

                # файл подготавливается, нужно пождать немного
                if response.status_code == 201:
                    time.sleep(3)

                elif response.status_code == 200:

                    # All in memory
                    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
                        for zipinfo in thezip.infolist():
                            with thezip.open(zipinfo) as thefile:
                                # print(thefile.read())

                    # Save to hdd
                    # with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
                    #     thezip.extractall(directory_to_extract_to)



                                root = ET.parse(thefile).getroot()

                                # <image id="0" name="20220215-090807/cvat/img/729_1_1.jpeg" width="932" height="1280">
                                #     <box label="bottle label" occluded="0" source="manual" xtl="54.04" ytl="468.00" xbr="275.32" ybr="898.81" z_order="0">
                                #     </box>
                                # </image>
                                tags_image = root.findall('image')
                                print(f'Найдено тегов картинок: {len(tags_image)}')
                                # Тэг с картинкой может быть, а координат внутри нет, поэтому нужно считать только размеченные
                                labeled_images_counter = 0
                                labels_counter = 0
                                for tag_image_counter, tag_image in enumerate(tags_image, 1):


                                    path_to_image = os.path.join(DRINKS_DATA_DIR, tag_image.get('name'))
                                    print(f'Картинка tag_image_counter: {tag_image_counter}, path_to_image: {path_to_image}')

                                    image = cv2.imread(path_to_image)
                                    # image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                                    # plt.imshow(image)
                                    # plt.show()

                                    path_to_image_dir, image_file = os.path.split(path_to_image)
                                    path_to_cvat_dir = os.path.dirname(path_to_image_dir)
                                    path_to_labels_dir = os.path.join(path_to_cvat_dir, 'labels')

                                    if not os.path.exists(path_to_labels_dir):
                                        os.makedirs(path_to_labels_dir)

                                    tags_box = tag_image.findall('box')
                                    
                                    if len(tags_box) > 0:
                                        labeled_images_counter += 1
                                        
                                    labels_counter += len(tags_box)
                                    
                                    for box_id, box in enumerate(tags_box):

                                        xtl = round(float(box.get('xtl')))
                                        ytl = round(float(box.get('ytl')))
                                        xbr = round(float(box.get('xbr')))
                                        ybr = round(float(box.get('ybr')))
                                        print(f'Координаты box_id:{box_id}, xtl:{xtl}, ytl:{ytl}, xbr:{xbr}, ybr:{ybr}')

                                        crop_img = image[ytl:ybr, xtl:xbr]

                                        try:    
                                            filename, file_extension = os.path.splitext(image_file)
                                            image_filename = f'{filename}_{box_id}{file_extension}'
                                            path_to_label = os.path.join(path_to_labels_dir, image_filename)
                                            print('Вырезанная этикетка: ', path_to_label)
                                            cv2.imwrite(path_to_label, crop_img)
                                        except Exception as e:
                                            print(e)
                                            
                                print(f'Всего размеченных картинок в cvat задаче было: {labeled_images_counter}')
                                print(f'Всего этикеток в cvat задаче было: {labels_counter}')
                    
                    
                    
                                dag_task_images_total_counter += len(tags_image)
                                dag_task_labeled_images_total_counter += labeled_images_counter
                                dag_task_labeles_total_counter += labels_counter
                                tasks_processed_total_counter += 1
                    # Завершение 200 
                    break                

    
    
        telegram(f'*Airflow Drinks ETL DAG*\n* cvat exported crop lable*\n```  CVAT TASKS STATUSES\n   annotation: {len(cvat_tasks_annotation):>5}\n   validation: {len(cvat_tasks_validation):>5}\n   complete: {len(cvat_tasks_complete):>7}\n\n  AIRFLOW TOTAL TASKS\n   processed: {len(tasks_processed)+tasks_processed_total_counter:>6}\n\n  AIRFLOW DAG TASKS\n   processed: {tasks_processed_total_counter:>6}\n   images total: {dag_task_images_total_counter:>3}\n   images labled: {dag_task_labeled_images_total_counter:>2}\n   lables total: {dag_task_labeles_total_counter:>3}```')

        
        # Если False, то дальше все таски скипаются (см. ShortCircuitOperator)
        return dag_task_labeles_total_counter > 0
            
        
        
        
        
    # cvat_exported_crop_lable_task = cvat_exported_crop_lable()

    cvat_exported_crop_lable_task = ShortCircuitOperator(
        task_id="cvat_exported_crop_lable",
        python_callable=cvat_exported_crop_lable,
        provide_context=True,
        op_kwargs={},
        dag=dag
    )



    
    def load_all_image_from_path_with_names(glob_path):
        image_list = []
        names = []
        for file_path in glob.glob(glob_path):
            class_name = file_path.split(sep='/')[-1].split('_')[0]
            image = cv2.imread(file_path, cv2.IMREAD_GRAYSCALE)
            image_list.append(image)
            names.append(int(class_name))
        print('classes count', len(names))
        return image_list, names
    
    
    
    
    @task(task_id='create_ngt_index')
    def create_ngt_index():
        print('create_ngt_index')
        
        train_image_list, names = load_all_image_from_path_with_names(f"{DRINKS_DATA_DIR}/cvat/*/labels/*")
               
        feature_extractor = cv2.xfeatures2d.SIFT_create(2000, edgeThreshold=10)

        des_train_list = []

        for i, train_image in enumerate(train_image_list):
            
            kp_train, des_train = feature_extractor.detectAndCompute(train_image, None)
             
            if des_train is None:
                print('WARN No description for index:', i, 'name:', names[i])
                #plt.imshow(train_image, cmap='gray'),plt.show()
                #shutil.rmtree(f'data/labels/images/{names[i]}')
                continue

            des_train_list.append(des_train)
        
        
        timestr = time.strftime('%Y%m%d-%H%M%S')
        timestamped_index_dir = f'{DRINKS_DATA_DIR}/index/{timestr}'
        ngt_dir = f'{timestamped_index_dir}/ngt'
        if not os.path.exists(ngt_dir):
            os.makedirs(ngt_dir)        
        names_pickle_file = f'{timestamped_index_dir}/names.pickle'

        ngtpy.create(ngt_dir.encode(), 128, distance_type = 'Angle', edge_size_for_creation=50, edge_size_for_search=100)
        index = ngtpy.Index(ngt_dir.encode())

        idx_names = []
        for des, name in zip(des_train_list, names):
            for d in des:
                index.insert(d)
                idx_names.append(name)

                
        index.build_index()
        index.save()
        index.close()
        pickle.dump(idx_names, open(names_pickle_file,'wb'))

        
        
        # telegram(f'*Airflow Drinks ETL DAG*\n* create ngt index*\n```  index dir:     {timestamped_index_dir.split("/")[-1]}\n  sift nfeatures:           2000\n  sift edgeThreshold:         10\n  ngt dimension:             128\n  ngt distance type:       Angle\n  ngt edge creation size:     50\n  ngt edge search size:      100```')
        
        telegram(f'*Airflow Drinks ETL DAG*\n* create ngt index*\n```  index dir:     {timestamped_index_dir.split("/")[-1]}```')
        
        
        
        
    create_ngt_index_task = create_ngt_index()
    
    
    
    
    
    @task(task_id='deploy_ngt_index')
    def deploy_ngt_index():
        print('deploy_ngt_index')
        
        
        update_url = Variable.get('drinks_api_update_url') 
        response = requests.get(update_url, verify=False)
        print(f'Drinks API response: {response}')
        print(f'Drinks API response: {response.text}')

        telegram(f'*Airflow Drinks ETL DAG*\n* deploy ngt index*\n```  response code: {response.status_code}\n  {response.text}```')

        if response.status_code != 200:
            raise ValueError('Index deploy error')
        
        
    deploy_ngt_index_task = deploy_ngt_index()


    
    start_notification_task >> image_to_cvat_task
    image_to_cvat_task >> cvat_exported_crop_lable_task
    cvat_exported_crop_lable_task >> create_ngt_index_task
    create_ngt_index_task >> deploy_ngt_index_task
