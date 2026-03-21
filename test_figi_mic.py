import requests
import json

URL = "https://api.openfigi.com/v3/mapping"

def test():
    # 模拟代码逻辑：优先用 micCode
    job1 = {"idType": "TICKER", "idValue": "ABLV", "micCode": "XNAS"}
    # 模拟用户 curl：只用 exchCode
    job2 = {"idType": "TICKER", "idValue": "ABLV", "exchCode": "US"}
    
    jobs = [job1, job2]
    print(f"Sending jobs: {json.dumps(jobs, indent=2)}")
    
    resp = requests.post(URL, json=jobs, headers={"Content-Type": "application/json"})
    if resp.status_code == 200:
        print(f"Response:\n{json.dumps(resp.json(), indent=2)}")
    else:
        print(f"Error [{resp.status_code}]: {resp.text}")

if __name__ == "__main__":
    test()
