from docx import Document
from docx.shared import Inches
import io
import requests

url = "https://quickchart.io/chart?c={type:'bar',data:{labels:['a','b'],datasets:[{data:[1,2]}]}}"
resp = requests.get(url)
print(resp.status_code)

doc = Document()
img_stream = io.BytesIO(resp.content)
doc.add_picture(img_stream, width=Inches(6.0))
doc.save("test_qc.docx")
print("Done")
