# import configparser
import mapping
import os
import time
from multiprocessing import Pool, freeze_support
import math

seconds = time.time()
print("Seconds since epoch =",time.ctime(time.time()))
# config = configparser.ConfigParser()
# config.read('config.ini')
MAPPING_DATA_FILE = mapping.MAPPING_DATA_FILE

# only_in_right = "Combinational Key \n"
# only_in_left = ""
# summary = {
#     "right_count": 0,
#     "only_in_right": 0,
#     "only_in_left": 0,
#     "Fields_Having_difference_From_Right": {},
#     "Total_number_of_records_having_difference_from_right": 0
# }



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


def right_fields():
    right_field = {}
    for i in MAPPING_DATA_FILE:
        right_field[MAPPING_DATA_FILE[i]] = MAPPING_DATA_FILE[i]
    return right_field


def modify_left_data(leftFile, comparison_key,delimiter, header):
    print("modify_left_data")
    print("Seconds since epoch =", time.ctime(time.time()))
    count = 0
    key_count = 0
    left_data = {}
    for line in leftFile:
        count +=1
        print(count)
        if header and count == 1:
            continue
        key = ""
        line = line.replace("\n","")
        left = line.split(delimiter)
        if len(comparison_key) == 0:
            key = count
        else:
            for i in comparison_key:
                key += left[i] + "_"
        left_data[str(key)] = {}
        for j in MAPPING_DATA_FILE:
            left_data[str(key)][j] = left[j]
    return {"left_data": left_data, "count":count }


def modify_right_data(rightFile, comparison_key,delimiter, header,left_data ):
    print("modify_right_data")
    print("Seconds since epoch =", time.ctime(time.time()))
    count = 0
    header_data = None
    only_in_right = "Combinational Key \n"
    only_in_left = ""
    summary = {
        "right_count": 0,
        "only_in_right": 0,
        "only_in_left": 0,
        "Fields_Having_difference_From_Right": {},
        "Total_number_of_records_having_difference_from_right":0
    }
    data_summary = {}
    is_diff_flag = False
    for line in rightFile:
        count += 1
        print(count)
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
            for j in MAPPING_DATA_FILE:
                if left_data[str(key)][j] != right[MAPPING_DATA_FILE[j]]:
                    is_diff_flag = True
                    _header = header_data[MAPPING_DATA_FILE[j]]
                    if _header not in summary["Fields_Having_difference_From_Right"]:
                        summary["Fields_Having_difference_From_Right"][_header] = 0
                        data_summary[str(_header)] = " \nKey, Left file, Right File" + "\n"
                    summary["Fields_Having_difference_From_Right"][_header] += 1
                    data_summary[str(_header)] += str(key) + "," +str(left_data[str(key)][j]) + "," + str(right[MAPPING_DATA_FILE[j]]) + "\n"
            del left_data[str(key)]
        else:
            summary["only_in_right"] += 1
            only_in_right += str(key) + "\n"
        if is_diff_flag:
            summary["Total_number_of_records_having_difference_from_right"] += 1

    # for k in left_data:
    #     summary["only_in_left"] += 1
    #     only_in_left += str(key) + "\n"
    # # print(data_summary)
    # summary["right_count"] = count
    # summary["left_count"] = left_count
    # modify_output_for_txt(summary,only_in_left,only_in_right, data_summary)
    result = {
        "summary": summary,
        "only_in_left": only_in_left,
        "only_in_right": only_in_right,
        "data_summary": data_summary
    }
    return result


def modify_output_for_txt(summary,only_in_left,only_in_right, data_summary):
    print("modify_output_for_txt")
    out_folder = mapping.OUT_FOLDER
    out_folder_left = out_folder + "only_in_left.txt"

    out_folder_sum = out_folder + "summary.txt"

    _out_folder_left = open(out_folder_left, "w+")
    _out_folder_left.write(only_in_left)
    _out_folder_left.close()



    # write_files(out_folder, "only_in_left.txt", only_in_left)
    # write_files(out_folder, "only_in_right.txt", only_in_right)
    summary_sting = ""
    _summary_sting = ""

    for i in summary:
        if i == "Fields_Having_difference_From_Right":
            _summary_sting += "\nFields_Having_difference_From_Right, count \n"
            for j in summary[i]:
                _summary_sting += str(j) + "," + str(summary["Fields_Having_difference_From_Right"][str(j)]) + "\n"
        else:
            summary_sting += str(i) + ","+str(summary[i]) + "\n"

    _out_folder_sum = open(out_folder_sum, "w+")
    _out_folder_sum.write(summary_sting + "\n"+ _summary_sting)
    _out_folder_sum.close()

    for i in data_summary:
        out_folder_data = out_folder +str(i) + ".txt"
        _out_folder_data = open(out_folder_data, "w+")
        _out_folder_data.write(data_summary[i] + "\n")
        _out_folder_data.close()

    # write_files(out_folder, "summary.txt", summary_sting)


def set_parallel_parameter(rightFile, comparison_key,delimiter, header,left_data,per_core_count,cpuCount ):
    params = []
    sub_params = (rightFile, comparison_key,delimiter, header,left_data)
    print(cpuCount)
    for i in range(int(cpuCount)):
        initial_count = i * per_core_count
        final_count = (i+1) * per_core_count
        _sub_params = sub_params + ( initial_count, final_count, i)
        params.append(_sub_params)
    return params


def multi_processing(parameters, core):
    print("multi_processing")


def comparison():
    print("right_field")
    try:
        left_file = mapping.LEFT_FILE
        leftFile = read_files(left_file["location"],left_file["file_name"] )
        left_data = modify_left_data(leftFile,left_file["comparison_key"],left_file["delimiter"],left_file["is_header"])
        left_count = left_data["count"]
        left_data = left_data["left_data"]

        right_file = mapping.RIGHT_FILE
        # parallel processing
        rightFile = read_files(right_file["location"],right_file["file_name"] )
        cpuCount = int(os.cpu_count() / 2)
        num_lines = sum(1 for line in rightFile)
        per_core_count = math.ceil(num_lines / cpuCount)
        parameters = set_parallel_parameter(rightFile, right_file["comparison_key"], right_file["delimiter"],
                                        right_file["is_header"],left_data, per_core_count,cpuCount)

        parallel_results =multi_processing(parameters,cpuCount )
        # summary = modify_right_data(rightFile,right_file["comparison_key"],right_file["delimiter"],right_file["is_header"], left_data, left_count)

        # print(summary)
        print("Seconds since epoch =", time.ctime(time.time()))
    except Exception as err:
        print(f"Unexpected {err} = , {type(err)}")
        raise


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
        left_data ={}
        with open(location_left) as leftFile:
            left_Files = leftFile.readlines()

            for line in left_Files:
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
                left_data[str(key)] = {}
                for j in MAPPING_DATA_FILE:
                    left_data[str(key)][j] = left[j]
                if left_key_cnt == 50000:
                    print("comparison_right "+ time.ctime(time.time())+ "  " + str(left_count))
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
        print("-------------------------------------------S")
        print(summary)
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
    print("comparison_right ------------------------")
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
        right_Files = rightFile.readlines()
        for line in right_Files:
            count += 1
            # print(count)
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
                for j in MAPPING_DATA_FILE:
                    if left_data[str(key)][j] != right[MAPPING_DATA_FILE[j]]:
                        is_diff_flag = True
                        _header = header_data[MAPPING_DATA_FILE[j]]
                        if _header not in summary["Fields_Having_difference_From_Right"]:
                            summary["Fields_Having_difference_From_Right"][_header] = 0
                            # data_summary[str(_header)] = " \nKey, Left file, Right File" + "\n"
                            data_summary[str(_header)] = " \nKey, "+str(mapping.LEFT_FILE["file_name"]) + " , " + str(right_file["file_name"]) + "\n"

                        summary["Fields_Having_difference_From_Right"][_header] += 1
                        data_summary[str(_header)] += str(key) + "," + str(left_data[str(key)][j]) + "," + str(
                            right[MAPPING_DATA_FILE[j]]) + "\n"
                del left_data[str(key)]
            if is_diff_flag:
                summary["Total_number_of_records_having_difference_from_right"] += 1

    for k in left_data:
        summary["only_in_left"] += 1
        only_in_left += str(k) + "\n"
    print("summary[only_in_left] -----" +str(summary["only_in_left"]))
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



def left_right_comp(left_file,right_file):
    print("left_right_comp")



def test():
    cpuCount = os.cpu_count()/2
    right_file = mapping.RIGHT_FILE
    rightFile = read_files(right_file["location"], right_file["file_name"])
    # num_lines = sum(1 for line in rightFile)
    num_lines = 855957
    per_core_count = math.ceil(num_lines / cpuCount)
    parameters = set_parallel_parameter(rightFile, right_file["comparison_key"], right_file["delimiter"],
                                        right_file["is_header"],"left_data","left_count", per_core_count,cpuCount)

    for i in parameters:
        print(i)
    print(num_lines)


if __name__ == '__main__':
    comparison_left()