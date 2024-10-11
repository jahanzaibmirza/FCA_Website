import csv
import re
import zipfile
import requests,os
import scrapy


class FcaDataSpider(scrapy.Spider):
    name = "fca_data"

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }



    def start_requests(self):
        url='https://www.fca.gov/bank-oversight/call-report-data-for-download'
        yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)
    def parse(self, response):
        latest_file_link=response.xpath("//div[contains(@class,'usa-layout-docs-main_content')]//p//a[contains(@href,'zip')]/@href").get('')
        latest_file_name=response.xpath("//div[contains(@class,'usa-layout-docs-main_content')]//p//a[contains(@href,'zip')]/text()").get('').replace(' ','_')
        main_link= f'https://www.fca.gov{latest_file_link}'

        os.makedirs(f'Zip_Folder_{latest_file_name}', exist_ok=True)
        resp = requests.get(main_link)
        if resp.status_code == 200:
            with open(f"Zip_Folder_{latest_file_name}/{latest_file_name}.zip", "wb") as file:
                file.write(resp.content)

            with zipfile.ZipFile(f"Zip_Folder_{latest_file_name}/{latest_file_name}.zip", 'r') as zip_ref:
                zip_ref.extractall(f"Extracted_Folder_{latest_file_name}/{latest_file_name}")

            file_with_D =[]
            if os.path.exists(f"Extracted_Folder_{latest_file_name}/{latest_file_name}"):
                files = os.listdir(f"Extracted_Folder_{latest_file_name}/{latest_file_name}")
                for each_file in files:
                    if each_file.startswith('D'):
                        file_with_D.append(each_file)

            directory = f"Extracted_Folder_{latest_file_name}/{latest_file_name}"
            header = ['VARIABLE NAME', 'FIELD TYPE', 'POS.', 'VARIABLE DESCRIPTION','File Name']
            csv_exists = os.path.exists(f'institution_data_{latest_file_name}.csv')

            for txt_file in file_with_D[:]:
                file_path = os.path.join(directory, txt_file)
                with open(file_path, 'r') as file:
                    content = file.read()
                    lines = content.strip().split('\n')
                    data_rows = []
                    current_row = []
                    for line in lines[8:]:
                        line = line.replace('**', '')
                        end_line= line.strip()
                        if end_line.startswith('NOTE') or end_line.startswith('VARIABLES.') or end_line.startswith('-'):
                            continue
                        if end_line=='':
                            continue
                        if re.match(r'^( {18,}|\t{3,})', line):
                            if current_row:  # Ensure current_row is initialized before appending
                                current_row[-1] += ' ' + line.strip()  # Append the description
                        else:
                            # If there's an existing row, check if it is empty or whitespace before adding it
                            if current_row and any(current_row):  # Check if current_row is not empty or whitespace
                                data_rows.append(current_row)
                                current_row.append(txt_file)


                            # Split the line into parts
                            split_line = line.split(maxsplit=3)  # Split into 4 parts
                            if split_line and split_line[0].isdigit():
                                current_row = [None, None, split_line[0]]  # Assign None for NAME, TYPE, and use POS
                                if len(split_line) > 1:
                                    current_row.append(' '.join(split_line[1:]))
                                else:
                                    current_row.append('')
                            else:
                                # If the first value is not a digit, assume it's the NAME and proceed normally
                                if len(split_line) == 3:
                                    current_row = split_line
                                else:
                                    current_row = split_line[:3]  # First 3 items
                                    current_row.append(' '.join(split_line[3:]))

                    if current_row and any(current_row):
                        data_rows.append(current_row)
                        current_row.append(txt_file)


                    with open(f'output/fc_{latest_file_name}.csv', 'a', newline='') as csvfile:
                        csvwriter = csv.writer(csvfile)
                        if not csv_exists:
                            csvwriter.writerow(header)
                            csv_exists = True
                        csvwriter.writerows(data_rows)
                    print("Data has been written to 'csv' file ")





