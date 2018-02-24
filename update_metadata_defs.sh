
#!/bin/bash

OPTIND=1         # Reset in case getopts has been used previously in the shell.

display_help() {
    echo "Usage: $0 [option...] {d}" >&2
    echo
    echo "   -d, --main_dir   -- main path to package files"
    echo "   -p, --python path  -- path to python- ie run which python to find out"
    echo "   -n, --npm path  -- path to npm- ie run: npm bin -g to find out"
    echo
    echo " ***example usage: /home/ubuntu/metadata-mgmt-tool/run_job.sh -d /home/ubuntu/metadata-mgmt-tool/ -j grab_asset_field_defs.py -p /home/ubuntu/miniconda2/bin/python -c fieldConfig_grab_asset_field_defs_server.yaml"
    echo " ***example usage ./update_metadata_defs.sh -d  /Users/j9/Desktop/metadata-mgmt-tool -p /usr/local/bin/python2 -n /usr/local/bin/npm "
    exit 1
}
# Initialize our variables:
path_to_main_dir=""
python_path=""
run_env=""
npm_path=""

asset_fields_job="grab_asset_field_defs.py"
asset_fields_config="fieldConfig_grab_asset_field_defs.yaml"
data_dictionary_attachments_defs_job="grab_datadictionary_attachments_defs.py"
data_dictionary_attachments_defs_config="fieldConfig_existing_datadicts.yaml"
upload_screendoor_defs_job="upload_screendoor_responses.py"
upload_screendoor_defs_config="fieldConfig_import_wkbks.yaml"
push_public_version_of_master_dd_job="push_public_version_of_master_dd.py"
push_public_version_of_master_dd_config="fieldConfig_push_public_v_of_master_dd.yaml"
load_asset_fields_job="load_assest_fields_and_data_dict_attactments.py"
load_asset_fields_config="fieldConfig_load_asset_fields.yaml"
update_master_dd_job="update_master_data_dictionary.py"
update_master_dd_config="fieldConfigMasterDD.yaml"
get_nbeids_job="get_nbeids.py"
get_nbeids_config="fieldConfig_nbeid.yaml"
fail_notification_job="fail_notification.py"
fail_notication_config="fieldConfig_nbeid.yaml"



while getopts "h?:d:p:r:n:" opt; do
    case "$opt" in
    h|\?)
        display_help
        exit 0
        ;;
    d)  path_to_main_dir=$OPTARG
        ;;
    r)  run_env=$OPTARG
        ;;
    p)  python_path=$OPTARG
        ;;
    n)  npm_path=$OPTARG
        ;;
    esac
done
fail_notication_config="configs/"$run_env$"_"$fail_notication_config

if [ -z "$path_to_main_dir" ]; then
    echo "*****You must enter a path to main package directory****"
    display_help
    exit 1
fi
if [ -z "$python_path" ]; then
    echo "*****You must enter a path for python****"
    display_help
    exit 1
fi
if [ -z "$run_env" ]; then
    echo "*****You must enter a run env- this should be desktop or server****"
    display_help
    exit 1
fi
if [ -z "$npm_path" ]; then
    echo "*****You must enter a path for npm: try- npm bin -g****"
    display_help
    exit 1
fi

run_job_cmd=$path_to_main_dir"run_job.sh"
fail_notification_job=$path_to_main_dir"pydev/"$fail_notification_job


#first part - get the data
$npm_path run --prefix $path_to_main_dir output_csvs
if [ $? -eq 0 ]; then
    echo "Grabbed the asset fields successfully "
else
    echo FAIL
    $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Could NOT grab the asset fields" -d $path_to_main_dir
    exit 1
fi

(  exec $run_job_cmd -d $path_to_main_dir -j $load_asset_fields_job -p $python_path -c $run_env"_"$load_asset_fields_config )

if [ $? -eq 0 ]; then
    echo "Uploaded the asset fields successfully to the master dd"
else
    echo FAIL
    $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Could NOT upload the asset fields to the master dd" -d $path_to_main_dir
    exit 1
fi

(  exec $run_job_cmd -d $path_to_main_dir -j $update_master_dd_job -p $python_path -c $run_env"_"$update_master_dd_config )
if [ $? -eq 0 ]; then
    echo "Updated the master dd successfully "
else
    echo FAIL
    $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Could not update the master dd" -d $path_to_main_dir
    exit 1
fi
(  exec $run_job_cmd -d $path_to_main_dir -j $get_nbeids_job -p $python_path -c $run_env"_"$get_nbeids_config  )
if [ $? -eq 0 ]; then
    echo "Updated the nbeids successfully"
else
    echo FAIL
    $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Could not update nbeids" -d $path_to_main_dir
    exit 1
fi

## second part- upload all the data dictionary definitions ##
(  exec $run_job_cmd -d $path_to_main_dir -j $asset_fields_job  -p $python_path -c $run_env"_"$asset_fields_config  )
if [ $? -eq 0 ]; then
    echo "Asset Field Definitions update ran successfully"
else
    echo FAIL
    $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Did not update the Asset Field Definitions" -d $path_to_main_dir
    exit 1
fi
( exec $run_job_cmd -d $path_to_main_dir -j $data_dictionary_attachments_defs_job  -p $python_path -c $run_env"_"$data_dictionary_attachments_defs_config  )
if [ $? -eq 0 ]; then
   echo "Data Dictionary Attachment Definitions Update Ran Successfully"
else
    echo FAIL
    $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Did not update the Data Dictionary Attachment Definitions" -d $path_to_main_dir
    exit 1
fi
(  exec $run_job_cmd -d $path_to_main_dir -j $upload_screendoor_defs_job  -p $python_path -c $run_env"_"$upload_screendoor_defs_config  )
if [ $? -eq 0 ]; then
    echo "Screendoor Definitions Update Ran Successfully "
else
    echo FAIL
    $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Could NOT Screendoor Definitions" -d $path_to_main_dir
    exit 1
fi

(  exec $run_job_cmd -d $path_to_main_dir -j $push_public_version_of_master_dd_job  -p $python_path -c $run_env"_"$push_public_version_of_master_dd_config  )
if [ $? -eq 0 ]; then
   echo "Pushed Public Version of the Master Dataset to the Data Portal"
else
   echo FAIL
   $python_path $fail_notification_job -c $fail_notication_config -m "FAILED: Could NOT Push Public Version of the Master Dataset to the Data Portal" -d $path_to_main_dir
   exit 1
fi
