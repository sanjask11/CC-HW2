## CC HW2

### Overview
Generated 20,000 HTML files with up to 375 outgoing links per file and stored them in Google Cloud Storage. A python program was developed to read the files directly from the storage bucket, analyze the link structure, compute summary statistics for incoming and outgoing links and construct the corresponding directed graph. Using this graph the original iterative page rank algorithm was implemented and executed until convergence based on the specified stopping condition. The program was run both locally and in the google cloud shell to compare their performance and a separate test was included to verify the correctness of the page rank implementation independently of the generated dataset.

### Environment & Resources 
**Google Cloud Project :** primal-ivy-485619-r6 

**Bucket Name :** gs://san-hw2-cc 

**Prefix/Directory :** html-pages/ 

**Permission Command:** `gcloud storage buckets add-iam-policy-binding gs://san-hw2-cc \ --member="allUsers" \--role="roles/storage.objectViewer"`


### Steps to Run 
**Clone the repository**
Command : \
`git clone https://github.com/sanjask11/CC-HW2.git`\
        `cd CC-HW2`

**Install Dependencies** 
Command : \
`pip install -r requirements.txt`

**Local Authentication with Google Cloud**
Command : \
`gcloud auth login` \
`gcloud auth application-default login` \
`gcloud config set project primal-ivy-485619-r6`


**Run the Program Locally**
Command : \
 `/usr/bin/time -p python3 pagerank_bucket.py \
  	        --bucket san-hw2-cc \
  	        --prefix html-pages/ \
             --n 20000 \
   	         --workers 32`


**Run the Program on the Google Cloud Shell**
Command : \
`time python3 pagerank_bucket.py \
        --bucket san-hw2-cc \
        --prefix html-pages/ \
        --n 20000 \
         --workers 64`


**Run the test to verify the PageRank Algorithm**
Command : \
`pytest tests/test_pagerank.py`









