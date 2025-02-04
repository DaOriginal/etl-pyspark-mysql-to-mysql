#!/bin/bash

backdate=$(date "+%Y-%m-%d %H:%M:%S")

folder_to_backup=""
log_file=""

back_server=""
back_user=""
back_directory=""

exclude_list=(
    "*.gz"
    "final_pull*"
)

rsync_command=(rsync -avz)
for exclude in "${exclude_list[@]}"; do
    rsync_command+=(--exclude="$exclude")
done

$rsync_command "$folder_to_backup" "$back_user@$back_server:$back_directory" &>> "$log_file"

if [ $? -eq 0 ]; then
    echo "Backup completed successfully." | tee -a "$log_file"
else
    echo "Backup failed. Check the log for details." | tee -a "$log_file"
fi
