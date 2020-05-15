from requests import Session, request, HTTPError
from itertools import islice
import tqdm
import re
from bs4 import BeautifulSoup
from concurrent import futures
import os
from datetime import date, timedelta

REPORT_URL = "https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports/"
INDEX = "https://www.who.int/"


class Result:
    def __init__(self, name, status):
        self.name = name
        self.status = status

    def __repr__(self):
        return "{} -> {}".format(self.name, self.status)


def get_urls():
    urls = []
    r = request("GET", REPORT_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    soup_tags = soup.find_all('a', href=re.compile(r"/docs/default-source/coronaviruse/situation-reports/.*pdf.*"))
    for tag in soup_tags:
        if tag['href'] not in urls:
            urls.append(INDEX + tag['href'])
    url_date = [re.search(r"/([0-9]+)", url).group(1) for url in urls]
    return zip(urls, url_date)


def generate_url(duration=None, skip=None):
    """
    duration 如果是个数字，意味着返回多少天之内的数据。
    duration 如果是个列表，意味着返回列表包含的数据。
    duration 如果是None，意味着自动选择缺少的数据。
    """
    start = date(2020, 1, 21)
    targets_dates = [(start + timedelta(days=day)).strftime('%Y%m%d') for day in range((date.today() - start).days)]
    urls = get_urls()
    try:
        if skip is not None:
            assert isinstance(duration, int) and isinstance(skip, int) and duration is not None
            rvs_targets_dates = sorted(targets_dates, reverse=True)
            request_url_dates = islice(rvs_targets_dates, skip, skip + duration)
        elif duration is None:
            file_dates = getfile_dates()
            request_url_dates = [target for target in targets_dates if target not in file_dates]
        else:
            request_url_dates = [target for target in targets_dates if target not in duration]
    except AssertionError:
        print("skip and duration must be normal number int")
    except TypeError as e:
        print(e)
    for url, url_date in urls:
        if url_date in request_url_dates:
            yield url


def savepdf(url):
    try:
        ##how to retry
        status = 'Failed'
        filename = re.search(r".*/(.*)\?.*", url).group(1)
        response = request('GET', url, timeout=120)
        with open("./data/" + filename, "xb") as f:
            f.write(response.content)
        status = 'Succeed'
    except FileExistsError:
        status = 'Skip'
    except HTTPError as e:
        print(e.message)
    return Result(filename, status)


def savepdf_many(duration=None, skip=None):
    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_url = {executor.submit(savepdf, url): url for url in generate_url(duration, skip)}
        result_iter = futures.as_completed(future_to_url, timeout=120)
        result_iter = tqdm.tqdm(result_iter, total=len(future_to_url))
        for future in result_iter:
            try:
                result = future.result()
                print(result)
            except Exception as e:
                print(e)


def getfile_dates():
    root, dirs, files = next(os.walk('./data'))
    file_date = sorted([re.match(r"[0-9]*", file).group() for file in files])
    return file_date


if __name__ == "__main__":
    # todo : 为什么输出的东西会错行，我记得有个os.flush 但是忘记什么时候该用这个东西
    # todo : 最好把这个封装成一个命令行 或者是模块？？ 我觉得module可能更好一点,这个可以只用来更新数据
    # todo:this transform the pdf data into json, try a plot show by each country in the jupternotebook , and filter ...
    savepdf_many()
