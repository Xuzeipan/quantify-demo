import requests 
import json
from os.path import expanduser 
from requests.auth import HTTPBasicAuth


def sign_in():
    # Load credentials # 加载凭证
    # with open(expanduser('brain.txt')) as f:
    #     credentials = json.load(f)

    # Extract username and password from the list # 从列表中提取用户名和密码
    # username, password = credentials

# 从环境变量获取凭证（适配 GitHub Actions）
    import os
    username = os.getenv("WQ_USERNAME")
    password = os.getenv("WQ_PASSWORD")
    
    if not username or not password:
        raise ValueError("请设置 WQ_USERNAME 和 WQ_PASSWORD 环境变量")

    # Create a session object # 创建会话对象
    sess = requests.Session()

    # Set up basic authentication # 设置基本身份验证
    sess.auth = HTTPBasicAuth(username, password)

    # Send a POST request to the API for authentication # 向API发送POST请求进行身份验证
    response = sess.post('https://api.worldquantbrain.com/authentication')

    # Print response status and content for debugging # 打印响应状态和内容以调试
    print(response.status_code)
    print(response.json())
    return sess


sess = sign_in()



import pandas as pd
import requests

def get_datafields(s, searchScope, dataset_id: str = '', search: str = ''):
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']
    
    if len(search) == 0:
        url_template = f"https://api.worldquantbrain.com/data-fields?instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50&offset={{x}}"
        count = 100
    else:
        url_template = f"https://api.worldquantbrain.com/data-fields?instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}&limit=50&search={search}&offset={{x}}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        if datafields.status_code == 200:
            datafields_list.append(datafields.json()['results'])
        else:
            print(f"Error fetching data at offset {x}: {datafields.status_code}")
    
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    
    datafields_df = pd.DataFrame(datafields_list_flat)
    
    return datafields_df

import pandas as pd
import requests

def get_datafields(s, searchScope, dataset_id: str = '', search: str = ''):
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']
    
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" \
            f"&instrumentType={instrument_type}" \
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" \
            "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" \
            f"&instrumentType={instrument_type}" \
            f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" \
            f"&search={search}" \
            "&offset={x}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])
    
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    
    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df

searchScope = {'region': 'USA', 'delay': 1, 'universe': 'TOP3000','instrumentType': 'EQUITY'}
fundamental6 = get_datafields(s = sess, searchScope = searchScope, dataset_id = 'news12')

fundamental6 = fundamental6[fundamental6['type'] == 'MATRIX']
fundamental6.head()

datafields_list_fundamental6 = fundamental6['id'].values

alpha_list = []

for datafield in datafields_list_fundamental6:
    print(f"正在将如下Alpha表达式与setting封装")
    alpha_expression = f"group_rank({datafield}/cap, subindustry)"
    print(alpha_expression)
    simulation_data = {
        'type': 'REGULAR',
        'settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'universe': 'TOP3000',
            'delay': 1,
            'decay': 0,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False,
        },
        'regular': alpha_expression
    }
    
    alpha_list.append(simulation_data)

print(f"there are {len(alpha_list)} Alphas to simulate")

# 将Alpha一个一个发送至服务器进行回测,并检查是否断线，如断线则重连
##设置log
import logging
# Configure the logging setting
logging.basicConfig(filename='simulation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


from time import sleep
import logging

alpha_fail_attempt_tolerance = 15 # 每个alpha允许的最大失败尝试次数

# 从第6个元素开始迭代alpha_list
for alpha in alpha_list:
    keep_trying = True  # 控制while循环继续的标志
    failure_count = 0  # 记录失败尝试次数的计数器

    while keep_trying:
        try:
            # 尝试发送POST请求
            sim_resp = sess.post(
                'https://api.worldquantbrain.com/simulations',
                json=alpha  # 将当前alpha（一个JSON）发送到服务器
            )

            # 从响应头中获取位置
            sim_progress_url = sim_resp.headers['Location']
            logging.info(f'Alpha location is: {sim_progress_url}')  # 记录位置
            print(f'Alpha location is: {sim_progress_url}')  # 打印位置
            keep_trying = False  # 成功获取位置，退出while循环

        except Exception as e:
            # 处理异常：记录错误，让程序休眠15秒后重试
            logging.error(f"No Location, sleep 15 and retry, error message: {str(e)}")
            print("No Location, sleep 15 and retry")
            sleep(15)  # 休眠15秒后重试
            failure_count += 1  # 增加失败尝试次数

            # 检查失败尝试次数是否达到容忍上限
            if failure_count >= alpha_fail_attempt_tolerance:
                sess = sign_in()  # 重新登录会话
                failure_count = 0  # 重置失败尝试次数
                logging.error(f"No location for too many times, move to next alpha {alpha['regular']}")  # 记录错误
                print(f"No location for too many times, move to next alpha {alpha['regular']}")  # 打印信息
                break  # 退出while循环，移动到for循环中的下一个alpha