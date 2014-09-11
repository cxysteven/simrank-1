#!/bin/bash

CURRENT_DIR="$(dirname $0)";
source ${CURRENT_DIR}/../../../common/script/algo_db_common.sh;
source ${CURRENT_DIR}/../conf/push_data.conf;

function main()
{
    log_info "main():: main begin";
    
    ##  push files
	push_files "${REMOTE_MACHINE_LIST}" "${LOCAL_REMOTE_DATA_FILE_LIST}" "${REMOTE_READY_FILE_PATH}"
    [ $? -ne 0 ] && return 1;

    log_info "main():: main finish";
    return 0;
}

main >> ${LOG_FILE_PATH} 2>&1;

if [ $? -ne 0 ]
then
    send_email "${MAIL_ADDRESS_LIST}" "${MAIL_SUBJECT}" "`tail -100 ${LOG_FILE_PATH}`";
    exit 1;
fi

exit 0;
