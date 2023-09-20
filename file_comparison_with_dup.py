# import configparser
import mapping
import os
import time
from multiprocessing import Pool, freeze_support
import math

seconds = time.time()
print("Seconds since epoch =",time.ctime(time.time()))

MAPPING_DATA_FILE = mapping.MAPPING_DATA_FILE


def read_files(location, file_name):
    _location = str(location) + str(file_name)
    return open(_location, "r")


def write_files(location, file_name, data):
    _location = location + file_name
    _write = open(location, "w+")
    _write.write(data)
    _write.close()
    return True


def check_file(location):
    return os.path.exists(location)


def comparison_left():
    print("comparison_left")
    try:
        print("Seconds since epoch =", time.ctime(time.time()))
        data_summary = {}
        summary = {
            "right_count": 0,
            "only_in_left": 0,
            "Fields_Having_difference_From_Right": {},
            "Total_number_of_records_having_difference_from_right": 0
        }
        left_file = mapping.LEFT_FILE
        location_left = str(left_file["location"]) + str(left_file["file_name"])
        left_key_cnt = 0
        left_count = 0
        left_data = {}
        with open(location_left) as leftFile:
            #left_Files = leftFile.readlines()

            for line in leftFile:
                left_count +=1
                left_key_cnt +=1
                if left_file["is_header"] and left_count == 1:
                    continue
                key = ""
                line = line.replace("\n", "")
                left = line.split(left_file["delimiter"])
                if len(left_file["comparison_key"]) == 0:
                    key = left_count
                else:
                    for i in left_file["comparison_key"]:
                        key += left[i] + "_"
                if (str(key) not in left_data):
                    left_data[str(key)] = []
                    left_data[str(key)].append({"count_left":1})
                else:
                    left_data[str(key)][0]["count_left"] += 1
                temp_value = {}
                for j in MAPPING_DATA_FILE:
                    temp_value[j] = left[j]
                left_data[str(key)].append(temp_value)
                if left_key_cnt == 50000:
                    _summary = comparison_right(left_data, summary, data_summary)
                    summary = _summary["summary"]
                    data_summary = _summary["data_summary"]
                    left_data = {}
                    left_key_cnt = 0
        _summary = comparison_right(left_data, summary, data_summary)
        summary = _summary["summary"]
        summary["left_count"] =  left_count
        data_summary = _summary["data_summary"]
        out_folder = mapping.OUT_FOLDER
        _summary_sting =""
        summary_sting = ""
        # print("-------------------------------------------S")
        # print(summary)
        out_folder_sum = out_folder + "summary.txt"
        for i in summary:
            if i == "Fields_Having_difference_From_Right":
                _summary_sting += "\nFields_Having_difference_From_Right, count \n"
                for j in summary[i]:
                    _summary_sting += str(j) + "," + str(summary["Fields_Having_difference_From_Right"][str(j)]) + "\n"
            else:
                summary_sting += str(i) + "," + str(summary[i]) + "\n"

        _out_folder_sum = open(out_folder_sum, "w+")
        _out_folder_sum.write(summary_sting + "\n" + _summary_sting)
        _out_folder_sum.close()
        print(summary["right_count"])
        print("Seconds since epoch005 =", time.ctime(time.time()))
    except Exception as err:
        print(f"Unexpected {err} = , {type(err)}")
        raise


def comparison_right(left_data, summary, data_summary):
    only_in_left = ""
    right_file = mapping.RIGHT_FILE
    delimiter = right_file["delimiter"]
    header = right_file["is_header"]
    comparison_key= right_file["comparison_key"]
    location_right = str(right_file["location"]) + str(right_file["file_name"])
    count = 0
    header_data = None

    is_diff_flag = False
    with open(location_right) as rightFile:
        #right_Files = rightFile.readlines()
        for line in rightFile:
            count += 1
            is_diff_flag = False
            line = line.replace("\n", "")
            right = line.split(delimiter)
            if header and count == 1:
                header_data = right
                continue
            if count == 1:
                header_data = [i for i in range(len(right))]
            key = ""
            if len(comparison_key) == 0:
                key = count
            else:
                for i in comparison_key:
                    key += right[i] + "_"
            if str(key) in left_data:
                if "count_right" in left_data[str(key)][0]:
                    left_data[str(key)][0]["count_right"] += 1
                else:
                    left_data[str(key)][0]["count_right"] = 1
                for _dup_data in range(1, len(left_data[str(key)])):
                    dup_data = left_data[str(key)][_dup_data]
                    for j in MAPPING_DATA_FILE:
                        if dup_data[j] != right[MAPPING_DATA_FILE[j]]:
                            is_diff_flag = True
                            _header = header_data[MAPPING_DATA_FILE[j]]
                            if _header not in summary["Fields_Having_difference_From_Right"]:
                                summary["Fields_Having_difference_From_Right"][_header] = 0
                                # data_summary[str(_header)] = " \nKey, Left file, Right File" + "\n"
                                data_summary[str(_header)] = " \nKey, "+str(mapping.LEFT_FILE["file_name"]) + " , " + str(right_file["file_name"]) + "\n"
    
                            summary["Fields_Having_difference_From_Right"][_header] += 1
                            data_summary[str(_header)] += str(key) + "," + str(dup_data[j]) + "," + str(
                            right[MAPPING_DATA_FILE[j]]) + "\n"
                if left_data[str(key)][0]["count_right"] == left_data[str(key)][0]["count_left"]:
                    del left_data[str(key)]
            if is_diff_flag:
                summary["Total_number_of_records_having_difference_from_right"] += 1
    for k in left_data:
        summary["only_in_left"] += 1
        only_in_left += str(k) + "\n"
    # print("summary[only_in_left] -----" +str(summary["only_in_left"]))
    out_folder = mapping.OUT_FOLDER
    out_folder_right = out_folder + "only_in_left.txt"
    _out_folder_right = open(out_folder_right, "a")
    _out_folder_right.write(only_in_left)
    _out_folder_right.close()
    only_in_left = ""
    summary["right_count"] = count

    for i in data_summary:
        if data_summary[i] == "":
            continue
        out_folder_data = out_folder + str(i.replace('"','')) + ".txt"
        _out_folder_data = open(out_folder_data, "a")
        _out_folder_data.write(data_summary[i] + "\n")
        _out_folder_data.close()
        data_summary[i] = ""
    result = {"summary": summary, "data_summary": data_summary }
    print(summary)
    return result
        # exit(0)



if __name__ == '__main__':
    print("zzzzzzzzzzzzzzzz")
    comparison_left()