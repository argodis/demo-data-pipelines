from ray import workflow
from typing import List


WORKFLOW_STORAGE = "/dbfs/mnt/data/alpaca/workflows"

@workflow.step
def read_data(num: int):
    return [i for i in range(num)]

@workflow.step
def preprocessing(data: List[float]) -> List[float]:
    return [d**2 for d in data]

@workflow.step
def aggregate(data: List[float]) -> float:
    return sum(data)

# Initialize workflow storage.
workflow.init(storage=WORKFLOW_STORAGE)

for i in range(100):
    # Setup the workflow.
    data = read_data.step(i)
    preprocessed_data = preprocessing.step(data)
    output = aggregate.step(preprocessed_data)

    # Execute the workflow and print the result.
    print(output.run_async())
