import requests

BASE_URL = "http://127.0.0.1:5000/"

response = requests.put(
    BASE_URL + "video/21", {"name": "cat video", "views": 40, "likes": 40}
)

print(response.json())

response_1 = requests.patch(BASE_URL + "video/21", {"name": "cat video 2"})
print(response_1.json())

response_3 = requests.get(BASE_URL + "video/21")
print(response_3.json())
