import pandas as pd
import csv

submission_df = pd.read_csv("./submission/unsorted_ungrouped_submission.csv")


submission_df.sort_values(by=["display_id", "ad_id", "predicted_label", "predicted_proba"],
                            ascending=[False, False, False, False], inplace=True)

print(submission_df.head())

submission_df.drop(["predicted_label", "predicted_proba"], axis=1, inplace=True)

submission_group_by = submission_df.groupby("display_id")["ad_id"].apply(list)

submission_dict = {}

print("**>> ")
print(len(submission_group_by))
print("**>> ")
# create a dictionary to hold display ids and unique ad ids
for index, row in submission_group_by .iteritems():
    unique_lst = []
    for i in row:
        if i not in unique_lst:
            unique_lst.append(i)
    submission_dict[index] = unique_lst

# write to the submission file
with open("./submission/final_submission.csv", "w") as submission_file_handle:
    file_writer = csv.writer(submission_file_handle, delimiter=',', quoting=csv.QUOTE_NONE, quotechar='',
                             lineterminator='\n')
    file_writer.writerow(["display_id", "ad_id"])
    for key, value in submission_dict.items():
        ad_id_str = ""
        for i, v in enumerate(value):
            if i != 0:
                ad_id_str += " " + str(v)
            else:
                ad_id_str += str(v)

        file_writer.writerow([key, ad_id_str])