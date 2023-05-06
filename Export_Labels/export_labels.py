import labelbox as lb
import argparse
import requests
import ndjson
import cv2
import yaml
import altFile as af

class ExportLabel(object):
    def __init__(self, api_key, project_id, yaml_path, destination_path, train_split):
        '''
        input:
        api key, 
        project id, 
        yaml path for getting the ref list of classifications, 
        destination path of labels
        '''
        self.api_key = api_key
        self.project_id = project_id
        self.destination_path = destination_path
        self.train_split = train_split
        with open(yaml_path, 'r') as file:
            self.classification_ref = yaml.safe_load(file)

    def pull_datarows(self):
        '''
        returns: list of [datarows] where each row, corresponds to a video and its annotation information in the labelbox format
        '''
        print("pulling data rows..")
        self.client = lb.Client(api_key=self.api_key)
        self.project = self.client.get_project(self.project_id)
        export_url = self.project.export_labels()
        labels = requests.get(export_url).json()
        print("Labels fetched..", len(labels))
        datarows = []
        for label in labels:
            datarow = {}
            if label["Label"].get("frames"):
                annotation_url = label["Label"]["frames"]
                headers = {"Authorization": f"Bearer {self.api_key}"}
                annotations = ndjson.loads(requests.get(annotation_url, headers=headers).text)
                datarow["Datarow_ID"] = label["DataRow ID"]
                datarow["annotations"] = annotations
                datarow["video_url"] = label["Labeled Data"]
                datarows.append(datarow)
        print("Labels skipped to convert to datarows..", len(labels)-len(datarows))
        return datarows       

    def pull_frames(self, video_link, frame_exclusions=[], last_frame_number=float("inf")):
        '''
        input: a video link and frame exclusions which are frame numbers that are not supposed to be saved (for purpose of missing annotations)
        returns: a list of images as numpy arrays
            else a list containing [-1] if the video failed to be pulled
        '''
        vidcap = cv2.VideoCapture(video_link)
        success, image = vidcap.read()
        images = []
        frame_count = 1
        while success:
            if frame_count not in frame_exclusions and frame_count<=last_frame_number:
                images.append(image)
            success, image = vidcap.read()
            frame_count += 1
        return images if len(images)>0 else [-1]


    def build_class_dict(self, annotations):
        '''
        input:  list of annotaions per frame (a dictionary object)
        returns: a dictionary that includes <key:featureId, value:classification> for every featureId in the "annotations" object
            default classification is car
        '''
        class_dict = {}
        for annotation in annotations:
            for a_object in annotation["objects"]:
                feature_id = a_object["featureId"]
                classes = a_object["classifications"]
                if len(classes)==0:
                    class_dict[feature_id] = "car"
                else:
                    class_dict[feature_id] = classes[0]["answer"]["value"].lower()
        return class_dict

    def build_yolo_annotations(self, datarow, class_dict):
        '''
        input: [annotations], {class_dict<featureId,classification>}
        returns: a list of annotations (tuples where each tuple is < Datarow_ID , yolo_label_string >) and 
            a list of frame numbers that do not have any annotations attached
        '''
        yolo_annotations = []
        datarow_id = datarow["Datarow_ID"]
        annotations = datarow["annotations"]
        frame_exclusions = []
        frame_count = 1
        class_ref = self.classification_ref["names"]
        class_ref = [c.lower() for c in class_ref]
        for annotation in annotations:
            frame_number = annotation["frameNumber"]
            if frame_number!=frame_count:
                frame_exclusions += [f for f in range(frame_count, frame_number)]
                frame_count = frame_number+1
            else:
                frame_count+=1
            yolo_label_string = ""
            for a_object in annotation["objects"]:
                feature_id = a_object["featureId"]
                classification_number = class_ref.index(class_dict[feature_id])
                bbox = a_object["bbox"]
                yolo_label_string += str(classification_number)+ " " + str(bbox["top"]) + " " + str(bbox["left"]) + " " + str(bbox["height"]) + " " + str(bbox["width"])
                yolo_label_string += "\n"
            yolo_label_string = yolo_label_string.strip()
            yolo_annotations.append((str(datarow_id), yolo_label_string))
        return yolo_annotations, frame_exclusions, frame_number

    def center_and_normalize_annotations(self, images, annotations):
        '''
        input: list of numpy array (images), annotations dictionary
        returns: annotations dictionary with x, y as the center of the bounding box instead of the top left corner and normalized by height and width 
        '''
        if len(images)!=len(annotations):
            raise Exception("images size "+str(len(images))+" and annotations length "+str(len(annotations))+" do not match")
        image_index = 0
        for i in range(len(annotations)):
            # get bounding boxes of a frame
            datarow_id, yolo_label_string = annotations[i]
            yolo_label_string = yolo_label_string.split("\n")
            new_label_string = ""
            for label in yolo_label_string:
                class_number, top, left, height, width = label.split(" ")
                img_height, img_width = images[image_index].shape[:2]
                top, left, height, width = float(top)/img_height, float(left)/img_width, float(height)/img_height, float(width)/img_width
                top, left = (top+(height/2)), (left+(width/2))
                new_label_string += str(class_number)+ " " + str(top) + " " + str(left) + " " + str(height) + " " + str(width)
                new_label_string += "\n"
            new_label_string = new_label_string.strip()
            annotations[i] = (datarow_id, new_label_string)
            image_index+=1
        return annotations

    def run(self):
        '''
        combined functionality
        '''
        print("Let's make some yolo labels")
        af.make_directories(self.destination_path, "train", "test")
        datarows = self.pull_datarows()
        print("Datarows:", len(datarows))
        status = {}
        i = 0
        for datarow in datarows:
            #print progress
            print("progress ", i, end="\r")
            i+=1
            # get class dict
            class_dict = self.build_class_dict(datarow["annotations"])
            
            # get annotations and exclusions
            yolo_annotations, frame_exclusions, last_frame_number = self.build_yolo_annotations(datarow, class_dict)

            # get decomposed frames as a set of images
            images = self.pull_frames(datarow["video_url"], frame_exclusions, last_frame_number)

            normalized_yolo_annotations = self.center_and_normalize_annotations(images, yolo_annotations)

            if isinstance(normalized_yolo_annotations, int):
                print(normalized_yolo_annotations)
            #get the permutations of locations to save
            permutations = af.make_permutations(len(normalized_yolo_annotations), [0, 1], [1-self.train_split, self.train_split])

            # only move forward if the length of images and labels are the same else discard the data row
            if len(images)==len(yolo_annotations):
                images_paths = af.shuffle_dir([self.destination_path+"/images/test", self.destination_path+"/images/train"], permutations)
                label_paths = af.shuffle_dir([self.destination_path+"/labels/test", self.destination_path+"/labels/train"], permutations)
                af.write_yolo_annotations(label_paths, normalized_yolo_annotations)
                result = af.save_frames(images, images_paths, datarow["Datarow_ID"])
            
            # save status in a dict if it fails, for now it is only used to find the number of failures
            if result==-1:
                status[datarow["Datarow_ID"]] = "Failed to save frames"

        # print the number of failed conversions
        if len(status)!=0:
            print("Failed to save", len(status), "images and lables from the datarows")
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="export_labels.py")
    parser.add_argument("--api-key")
    parser.add_argument("--project-id")
    parser.add_argument("--yaml-path") # yaml file which has a list called "names" of different classes
    parser.add_argument("--destination-path") # destination path where you want the directories with labels
    parser.add_argument("--train-split", type=float) # probability distribution value to split the labels and the frames
    opt = parser.parse_args()
    u = ExportLabel(opt.api_key, opt.project_id, opt.yaml_path, opt.destination_path, opt.train_split)
    u.run()