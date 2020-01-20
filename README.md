# testcase_darksky
## Call *darksky_script.py*
### without args: current weather in the *cities* table
`python darksky_apiscript.py`
### 10 min weather in the city (int city_id in the testcase from 1 to 12)
`python darksky_apiscript.py --city_id {city_id}`
### convert to csv:
`python darksky_apiscript.py --fname {filename.csv}`