# need to install module: PyPDF2, pdfplumber, reportlab, spark
# Read xlsx data and Write pdf
# window에선 정상 동작하나 mac에선 정상동작하지 않음을 확인
from PyPDF2 import PdfFileReader, PdfFileWriter

from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import letter

import pdfplumber

from pyspark.sql import SparkSession

import io
import re
import os
import math

# spark session 생성
spark = SparkSession \
            .builder \
            .config('spark.driver.host', '127.0.0.1') \
            .getOrCreate()

# spark로 json 파일을 읽은 뒤 SQL View(이름은 'SKU')로 생성
spark.read.json('./SKU.json', multiLine=True).createOrReplaceTempView("SKU")

# 숫자~한글~, (숫자 4개) 패턴 필터링
regex = re.compile(r'\d*[가-힣]+.*|\(\d{4}\)')
# LZDID 필터링(보낼 송장 구별 텍스트)
regex2 = re.compile(r'LZDID|First Mile Warehouse')
# 숫자~한글~숫자~
regex3 = re.compile(r'\d*[가-힣].*\(\d{3,4}')
# 한글 + 숫자
regex4 = re.compile(r'^[가-힣0-9]+')
# (숫자)
regex5 = re.compile(r'\(*\d{3,4}\)*]')
# 한글
regex6 = re.compile(r'^[가-힣]+(?![가-힣]$)')
# '호' 로 끝나는 텍스트 필터링
regex7 = re.compile(r'.*호$')
# '트' 로 끝나는 텍스트 필터링
regex8 = re.compile(r'.*트$')
# 한글로 끝나는 텍스트 필터링
regex9 = re.compile(r'.[가-힣]$')

# 폰트 설정
pdfmetrics.registerFont(TTFont("D2Coding", "D2Coding.ttf"))
# 현재 경로 가져옴
currentPath = os.getcwd()
# 현재경로의 파일리스트
file_list = os.listdir(currentPath)
# 파일리스트에서 Seller Center(숫자).pdf 의 형태를 가진 파일만 추출해서 list로 만듬
file_list = list(filter(lambda s: re.search(r'^Seller Center\d+\.pdf$', s), file_list))
# 파일리스트 순회
for file_name in file_list:
    # pdf 작성
    output = PdfFileWriter()
    # pyPDF2를 통해 pdf를 읽어옴(merge용)
    exist_pdf = PdfFileReader(currentPath+'/'+file_name)
    # pdfplumber를 통해 pdf를 읽어옴(text추출용)
    pages = pdfplumber.open(currentPath+'/__'+file_name).pages
    # 텍스트 나열
    texts = ""
    # 상품 딕셔너리
    prod_dict = {}
    # 상품 리스트
    products = []

    for idx, page in enumerate(pages):
        texts += str(page.extract_text())

        checkInvoice = regex2.search(texts)
        result = regex.findall(texts)

        if checkInvoice is not None:
            # texts 를 통해 products 리스트 생성
            products = list(map(lambda s: re.sub(r'\x00', '0', s), products))
            products = list(map(lambda s: re.sub(r'\n', '', s), products))
            products = list(map(lambda s: re.sub(r'\d+[_]ID[-]', '', s), products))
            products = list(map(lambda s: s.strip(), products))
            products = list(map(lambda s: re.sub(r'\)*\s(?!.*[가-힣]$).*', ')', s), products))
            products = list(map(lambda s: re.sub(r',|\+', ' ', s), products))
            products = list(map(lambda s: (')' if bool(regex4.search(s)) &
                                                  bool(regex.search(s)) is False &
                                                  bool(regex4.search(s)) is False else '')+s, products))

            products = list(map(lambda s: s + (')' if bool(regex7.search(s)) |
                                                      bool(regex8.search(s)) |
                                                      bool(regex6.search(s)) else ''), products))
            total_text = "".join(products)
            products = total_text.split(')')
            products = list(map(lambda s: s+(')' if regex3.search(s) else ''), products))
            products = list(map(lambda s: s.strip(), products))
            products = list(map(lambda s: re.sub(r'\(*\d{4}\)*', '', s), products))
            products = list(map(lambda s: re.sub(r'\(', '', s), products))
            products = list(filter(None, products))
            products = list(filter(lambda s: bool(regex9.search(s)), products))

            for product in products:
                df = spark.sql("SELECT * FROM SKU WHERE NAME like '{}%'".format(product))
                data = df.rdd.take(1)

                if len(data) >= 1:      # 결과값이 있으면 첫 번째 값 반환
                    data = data[0]
                else:                   # 없으면 쿼리를 변경하여 재조회
                    df = spark.sql("SELECT * FROM SKU WHERE NAME like '%{}%'".format(product))
                    data = df.rdd.take(1)
                    data = data[0]

                dataText = str(data.NAME)
                if len(data.CODE) > 0:
                    dataText = dataText + " ({})".format(data.CODE)

                if dataText in prod_dict:
                    prod_dict[dataText] = prod_dict[dataText]+1
                else:
                    prod_dict[dataText] = 1

            # print(idx, prod_dict)

            packet = io.BytesIO()
            can = canvas.Canvas(packet, pagesize=letter)            # pdf 출력 letter 사이즈
            can.setFillColorRGB(255, 255, 255)                      # 캔버스 색상 흰 색
            can.setLineWidth(0.75)                                  # 라인 너비
            can.rect(6.45, 525, 272.3, 70, fill=1)                  # 사각형 생성
            can.setFillColorRGB(0, 0, 0)                            # 캔버스 색상 검정색

            i = 0                                                   # 딕셔너리 index
            for key, value in prod_dict.items():
                posY = 587.5 - (11 * math.floor(i / 3))              # text y축
                posX = 8                                            # text x축
                if i % 3 == 0:
                    posX = 8
                elif i % 3 == 1:
                    posX = 100
                else:
                    posX = 192

                writeText = '{} {}'.format(key, value)
                if len(writeText) < 15:
                    can.setFont("D2Coding", 8)  # 폰트종류: D2Coding, 폰트크기: 8
                elif len(writeText) >= 15 & len(writeText) < 17:
                    can.setFont("D2Coding", 7)  # 폰트종류: D2Coding, 폰트크기: 7
                else:
                    can.setFont("D2Coding", 6)  # 폰트종류: D2Coding, 폰트크기: 6

                can.drawString(posX, posY, writeText)
                i = i + 1

            can.save()
            packet.seek(0)
            new_pdf = PdfFileReader(packet)
            page2 = exist_pdf.getPage(idx)
            page2.mergePage(new_pdf.getPage(0))
            output.addPage(page2)

            # products 리스트 초기화
            products.clear()
            # prod_dict 딕셔너리 초기화
            prod_dict.clear()
        # check invoice 가 없고 정규표현식으로 찾은 result 값이 있다면
        elif result is not None and checkInvoice is None:
            products = products + result

        # texts 초기화
        texts = ""

    with open("_"+file_name, "wb") as outputStream:
        output.write(outputStream)
