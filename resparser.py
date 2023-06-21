import os
import multiprocessing as mp
import io
import spacy
import pprint
import utils
import json
import time
from pymongo import MongoClient

class ResumeParser(object):

    def __init__(
        self,
        resume,
    ):
        nlp = spacy.load('en_core_web_sm')  
        self.__details = {
            'id': None,
            'email': None,
            'mobile_number': None,
            'skills': None,
        }
        self.__resume = resume
        if not isinstance(self.__resume, io.BytesIO):
            ext = os.path.splitext(self.__resume)[1].split('.')[1]
        else:
            ext = self.__resume.name.split('.')[1]
        self.__text_raw = utils.extract_text(self.__resume, '.' + ext)
        self.__text = ' '.join(self.__text_raw.split())
        self.__nlp = nlp(self.__text)
        self.__noun_chunks = list(self.__nlp.noun_chunks)
        self.__get_basic_details()

    def get_extracted_data(self):
        return self.__details
    
    def save(self, filename="data.json"):
        with open(filename, 'w') as file:
            json.dump(self.__details, file, indent=4)

    def __get_basic_details(self):
        email = utils.extract_email(self.__text)
        mobile = utils.extract_mobile_number(self.__text)
        skills = utils.extract_skills(
                    self.__nlp,
                    self.__noun_chunks,
                )

        self.__details['email'] = email
        self.__details['mobile_number'] = mobile
        self.__details['skills'] = skills
        
        return


def resume_result_wrapper(resume, idx):
    parser = ResumeParser(resume)
    data = parser.get_extracted_data()
    data['id'] = idx
    return data


if __name__ == '__main__':
    start_time = time.time()
    pool = mp.Pool(mp.cpu_count())

    resumes = []
    data = []
    for root, directories, filenames in os.walk('resumes/'):
        for filename in filenames:
            file = os.path.join(root, filename)
            resumes.append(file)

    results = [
        pool.apply_async(
            resume_result_wrapper,
            args=(x, idx)
        ) for idx, x in enumerate(resumes)
    ]

    results = [p.get() for p in results]

    # Rename PDF files with their respective ID numbers
    for item in results:
        old_name = resumes[item['id']]
        new_name = os.path.splitext(old_name)[0] + f"_id{item['id']}" + os.path.splitext(old_name)[1]
        os.rename(old_name, new_name)

    pprint.pprint(results)
    with open("sample.json", "w") as outfile:
        json.dump(results, outfile)

    # Connect to MongoDB and upload results
    client = MongoClient('mongodb://localhost:27017/')
    db = client['resume']
    collection = db['resume']
    collection.insert_many(results)
    client.close()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Program executed in {execution_time:.2f} seconds.")
