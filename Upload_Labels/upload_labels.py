import labelbox as lb
import uuid
from test_label import sample_label
import argparse
import json
import os
import time


class UploadLabel(object):
    def __init__(self, api_key, labels, project_id, video_dataset_name, upload_labels, mode_upload_video, ontology_id="", video_directory_path=""):
        print("initialised")
        self.client = lb.Client(api_key=api_key)
        self.project = self.client.get_project(project_id)
        self.upload_labels_flag = upload_labels
        labels = json.load(labels)
        self.labels = labels if isinstance(labels, list) else [labels]
        self.mode_upload_video = mode_upload_video
        self.video_directory_path = video_directory_path
        self.video_dataset_name = video_dataset_name
        # if you want to get an ontology
        # self.ontology = client.get_ontology(ontology_id)
        # self.project.setup_editor(self.ontology)

    def upload_batch_videos(self, mode_upload_video):
        global_keys = os.listdir(self.video_directory_path) if mode_upload_video=="batch" else [os.path.basename(self.video_directory_path)]
        dataset = self.client.create_dataset(name=self.video_dataset_name)
        assets = [
            {
                "row_data": os.path.join(self.video_directory_path, global_key) if mode_upload_video=="batch" else self.video_directory_path, 
                "global_key": global_key,
                "media_type": "VIDEO"
            } for global_key in global_keys
        ]
        task = dataset.create_data_rows(assets)
        task.wait_till_done()
        print("Errors :",task.errors)
        print("Failed data rows:", task.failed_data_rows)
        batch = self.project.create_batch(
            self.video_dataset_name+"_"+str(int(time.time())), # Each batch in a project must have a unique name
            global_keys=global_keys, # A paginated collection of data row objects, a list of data rows or global keys
            priority=5 # priority between 1(Highest) - 5(lowest)
        )
        print(batch, self.video_dataset_name+"_"+str(int(time.time())))

    def upload_labels(self):
        upload_job_label_import = lb.LabelImport.create_from_objects(
            client = self.client,
            project_id = self.project.uid, 
            name = "label_import_job-" + str(uuid.uuid4()),
            labels=self.labels
        )
        upload_job_label_import.wait_until_done()
        print("Errors:", upload_job_label_import.errors)
        print("Status of uploads: ", upload_job_label_import.statuses)

            
    def run(self):
        if self.mode_upload_video in ["single", "batch"]:
            self.upload_batch_videos(self.mode_upload_video)
        if self.upload_labels_flag=="y":
            self.upload_labels()

if __name__ == '__main__':
    # flag for upplaoding videos , how to upload videos (in batch or just a single file)
    parser = argparse.ArgumentParser(prog="upload_labels.py")
    parser.add_argument("--api-key")
    parser.add_argument("--project-id")
    parser.add_argument("--ontology-id", default="")
    parser.add_argument("--label-file", type=argparse.FileType('r'))
    parser.add_argument("--upload-labels", choices=["y", "n"], default="y")
    parser.add_argument("--mode-upload-video", choices=['single', 'batch', 'none'])
    parser.add_argument("--video-directory-path", default="")
    parser.add_argument("--video-dataset-name", default="video_demo_dataset")
    opt = parser.parse_args()
    u = UploadLabel(opt.api_key, opt.label_file, opt.project_id, opt.video_dataset_name, opt.upload_labels, opt.mode_upload_video, opt.ontology_id, opt.video_directory_path)
    u.run()