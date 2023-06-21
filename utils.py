import io
import os
import re
import pandas as pd
import textract
import docx2txt
from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFSyntaxError


def extract_text_from_pdf(pdf_path):

    if not isinstance(pdf_path, io.BytesIO):
        # extract text from local pdf file
        with open(pdf_path, 'rb') as fh:
            try:
                for page in PDFPage.get_pages(
                        fh,
                        caching=True,
                        check_extractable=True
                ):
                    resource_manager = PDFResourceManager()
                    fake_file_handle = io.StringIO()
                    converter = TextConverter(
                        resource_manager,
                        fake_file_handle,
                        codec='utf-8',
                        laparams=LAParams()
                    )
                    page_interpreter = PDFPageInterpreter(
                        resource_manager,
                        converter
                    )
                    page_interpreter.process_page(page)

                    text = fake_file_handle.getvalue()
                    yield text

                    # close open handles
                    converter.close()
                    fake_file_handle.close()
            except PDFSyntaxError:
                return
    else:
        # extract text from remote pdf file
        try:
            for page in PDFPage.get_pages(
                    pdf_path,
                    caching=True,
                    check_extractable=True
            ):
                resource_manager = PDFResourceManager()
                fake_file_handle = io.StringIO()
                converter = TextConverter(
                    resource_manager,
                    fake_file_handle,
                    codec='utf-8',
                    laparams=LAParams()
                )
                page_interpreter = PDFPageInterpreter(
                    resource_manager,
                    converter
                )
                page_interpreter.process_page(page)

                text = fake_file_handle.getvalue()
                yield text

                # close open handles
                converter.close()
                fake_file_handle.close()
        except PDFSyntaxError:
            return


def extract_text_from_docx(doc_path):
    try:
        temp = docx2txt.process(doc_path)
        text = [line.replace('\t', ' ') for line in temp.split('\n') if line]
        return ' '.join(text)
    except KeyError:
        return ' '


def extract_text_from_doc(doc_path):
    try:
        text = textract.process(doc_path).decode('utf-8')
        return text
    except KeyError:
        return ' '


def extract_text(file_path, extension):    
    text = ''
    if extension == '.pdf':
        for page in extract_text_from_pdf(file_path):
            text += ' ' + page
    elif extension == '.docx':
        text = extract_text_from_docx(file_path)
    elif extension == '.doc':
        text = extract_text_from_doc(file_path)
    return text


def extract_email(text):
    email = re.findall(r"([^@|\s]+@[^@]+\.[^@|\s]+)", text)
    if email:
        try:
            return email[0].split()[0].strip(';')
        except IndexError:
            return None


def extract_mobile_number(text):
    mob_num_regex = r'''(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)
                    [-\.\s]*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})'''
    phone = re.findall(re.compile(mob_num_regex), text)
    
    if phone:
        number = ''.join(phone[0])
        return number


def extract_skills(nlp_text, noun_chunks, skills_file=None):
    if not skills_file:
        skills_file = os.path.join(os.path.dirname(__file__), 'skills.csv')

    data = pd.read_csv(skills_file)
    skills = set(data.columns.str.lower().str.replace(' ', ''))

    skillset = set()
    for token in nlp_text:
        if not token.is_stop:
            token_text = token.text.lower().replace(' ', '')
            if token_text in skills:
                skillset.add(token.text.capitalize())

    for chunk in noun_chunks:
        chunk_text = chunk.text.lower().replace(' ', '')
        if chunk_text in skills:
            skillset.add(chunk.text.capitalize())

    return list(skillset)


