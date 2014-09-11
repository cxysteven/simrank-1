#!/bin/sh
#####################################################################################
CURRENT_DATE=$1;
CURRENT_DIR=$(dirname $0);
source $CURRENT_DIR/../../../common/script/algo_db_common.sh;
source $CURRENT_DIR/../conf/main.conf;


################################# 初始化全局变量  ###################################
##  


################### 删除过期的数据,删除3天的数据，【PRESERVE_DAYS,PRESERVE_DAYS+3）天的数据。##########
function delete_expired_data()
{
    log_info "delete_expired_data():: begin";
    for((i=${PRESERVE_DAYS};i<${PRESERVE_DAYS}+3;i++))
    do
        local tmp_date=`date -d "$CURRENT_DATE $i days ago" +"%Y%m%d"`;
        hadoop_secure_rmr ${HADOOP_OUTPUT_BASE_DIR}/${tmp_date}/query_pv;
        [ $? -ne 0 ] && return 1;
    done

    log_info "delete_expired_data():: finished";
    return 0;
}

function get_one_date_query_pv()
{
    log_info "get_one_date_query_pv():: begin";
    
    for((i=0;i<${PROCESS_DAYS};i++))
    do
        local tmp_date=`date -d "$CURRENT_DATE $i days ago" +"%Y%m%d"`;
        local hadoop_tmp_output_dir="${HADOOP_OUTPUT_BASE_DIR}/${tmp_date}/query_pv/output/one_day_query_pv";
        log_info "get_one_date_query_pv():: hadoop_tmp_output_dir=${hadoop_tmp_output_dir}";

        hadoop_get_file_count "${hadoop_tmp_output_dir}/*part*";
        local file_num=${HADOOP_FILE_COUNT};

        ## 如果${hadoop_tmp_output_dir}已经有数据，则不再生成
	## 默认重跑当天的数据
        if [ ${file_num} -eq 0 ] || [ $i -eq 0 ]
        then
            hadoop_tmp_input_path=${HADOOP_P4P_PV_LOG_BASE_DIR}/${tmp_date}/*/*/*/*.log;
            hadoop_secure_rmr ${hadoop_tmp_output_dir};

            local cmd="${HADOOP_EXEC} jar  \
	    -libjars ${LIB_JARS},${JAR_NORM_PATH} \
            ${JAR_QUERY_PV_PATH} \
            ${CLASS_LAUNCHER} \
            ${CLALL_QUERY_PV} \
            ${hadoop_tmp_input_path} \
            ${hadoop_tmp_output_dir} \
            1 \
            ${REDUCE_NUM} \
            true \
            pids=${PIDS} \
	    url=${ALIWS_URL} \
	    normalize_option=${NORMALIZE_OPTION} \
	    prop_num=${PROP_NUM} \
	    cat_num=${CAT_NUM}";
            log_info ${cmd};
            eval ${cmd};

            if [ $? -ne 0 ] 
            then
                log_warn "get_one_date_query_pv():: failed";
                hadoop_secure_rmr ${hadoop_tmp_output_dir};
                continue;
            else
                log_info "get_one_date_query_pv():: success";
            fi  
        fi

    done

    log_info "get_one_date_query_pv():: finished";
    return 0;
}

function get_multi_date_query_pv()
{
    accumulate_days=$1;
    min_pv=$2;
    hadoop_output_multi_date_query_pv_dir=$3;
    log_info "get_multi_date_query_pv():: begin, accumulate_days=$1,min_pv=$2,hadoop_output_multi_date_query_pv_dir=$3";

    local hadoop_tmp_input_multi_date_query_pv_dirs="";
    for((i=0;i<${accumulate_days};i++))
    do
        local tmp_date=`date -d "$CURRENT_DATE $i days ago" +"%Y%m%d"`;
        local hadoop_tmp_output_dir="${HADOOP_OUTPUT_BASE_DIR}/${tmp_date}/query_pv/output/one_day_query_pv";
        log_info "get_multi_date_query_pv():: hadoop_tmp_output_dir=${hadoop_tmp_output_dir}";
        
	##########################拼接目录 ##################
        hadoop_get_file_count "${hadoop_tmp_output_dir}/*part*";
        file_num=${HADOOP_FILE_COUNT};

        if  [ ${file_num} -ne 0 ];
        then
            if [ "$hadoop_tmp_input_multi_date_query_pv_dirs" == "" ];
            then    
                hadoop_tmp_input_multi_date_query_pv_dirs="${hadoop_tmp_output_dir}/*part*";
            else
                hadoop_tmp_input_multi_date_query_pv_dirs="${hadoop_tmp_input_multi_date_query_pv_dirs},${hadoop_tmp_output_dir}/*part*";
            fi  
        fi
    done
    log_info "get_multi_date_query_pv():: hadoop_tmp_input_multi_date_query_pv_dirs=${hadoop_tmp_input_multi_date_query_pv_dirs}";
    
    hadoop_secure_rmr ${hadoop_output_multi_date_query_pv_dir};
    local cmd="${HADOOP_EXEC} jar -libjars ${LIB_JARS} \
    ${JAR_QUERY_PV_PATH} \
    ${CLASS_LAUNCHER} \
    ${CLALL_ACCUMULATE} \
    ${hadoop_tmp_input_multi_date_query_pv_dirs} \
    ${hadoop_output_multi_date_query_pv_dir} \
    1 \
    ${REDUCE_NUM} \
    false \
    min_pv=${min_pv} \
    mapred.child.java.opts=${JAVA_OPTS}";
    
    log_info ${cmd};
    eval ${cmd};

    if [ $? -ne 0 ] 
    then
        log_error "get_multi_date_query_pv():: gen ${hadoop_output_multi_date_query_pv_dir} failed"
        return 1;
    else
        log_info "get_multi_date_query_pv():: gen ${hadoop_output_multi_date_query_pv_dir} finished";
        return 0;
    fi
}

function get_multi_date_query_norm()
{
    log_info "get_multi_date_query_norm():: begin";
    
    hadoop_secure_rmr ${HADOOP_OUTPUT_MULTI_DATE_QUERY_NORM_DIR};

    local cmd="${HADOOP_EXEC} jar -libjars ${LIB_JARS}\
    ${JAR_QUERY_PV_PATH} \
    ${CLASS_LAUNCHER} \
    ${CLASS_QUERYNORM} \
    ${HADOOP_OUTPUT_WEEK_MULTI_DATE_QUERY_PV_DIR}/kk* \
    ${HADOOP_OUTPUT_MULTI_DATE_QUERY_NORM_DIR} \
    1 \
    ${REDUCE_NUM} \
    false ";
    
    log_info ${cmd};
    eval ${cmd};

    if [ $? -ne 0 ] 
    then
        log_error "get_multi_date_query_norm():: failed"
        return 1;
    else
        log_info "get_multi_date_query_norm():: finished";
        return 0;
    fi
}

function download_data()
{
    log_info "download_data():: begin";

    for hadoop_local_data_file in ${HADOOP_2_LOCAL_DATA_FILE_LIST};
    do  
        local hadoop_data_file=`echo ${hadoop_local_data_file} | awk -F: '{print $1;}'`;
        local local_data_file=`echo ${hadoop_local_data_file} | awk -F: '{print $2;}'`;

        ${HADOOP_EXEC} dfs -cat ${hadoop_data_file} > ${local_data_file};
        if [ $? -ne 0 ] 
        then
            log_error "download_data():: from ${hadoop_data_file} to ${local_data_file} failed";
            return 1;
        fi
    done

    log_info "download_data():: finished";
    return 0;
}

function monitor()
{
    log_info "monitor():: begin";

    for file_and_num in ${MONITOR_TXT_FILE_LIST};
    do  
        local file=$(echo ${file_and_num} | awk -F: '{print $1;}');
        local num=$(echo ${file_and_num} | awk -F: '{print $2;}');
        check_txt_file_min_lines ${file} ${num};
        if [ $? -ne 0 ] 
        then
            log_error "monitor():: failed"
            return 1;
        fi
    done

    log_info "monitor():: finished";
    return 0;
}

function main()
{
   log_info "main():: begin";

   delete_expired_data;
   [ $? -ne 0 ] && return 1;
  
   get_one_date_query_pv;
   [ $? -ne 0 ] && return 1;
   
   get_multi_date_query_pv ${WEEK_ACCUMULATE_DAYS} ${WEEK_MIN_PV} ${HADOOP_OUTPUT_WEEK_MULTI_DATE_QUERY_PV_DIR};
   [ $? -ne 0 ] && return 1;
   
   get_multi_date_query_pv ${MONTH_ACCUMULATE_DAYS} ${MONTH_MIN_PV} ${HADOOP_OUTPUT_MONTH_MULTI_DATE_QUERY_PV_DIR};
   [ $? -ne 0 ] && return 1;

   get_multi_date_query_norm;
   [ $? -ne 0 ] && return 1;

   download_data;
   [ $? -ne 0 ] && return 1;

   monitor;
   [ $? -ne 0 ] && return 1;

    log_info "main():: finished";
    return 0;
}

main >> ${LOG_FILE_PATH} 2>&1;

if [ $? -ne 0 ] 
then
    send_email "${MAIL_ADDRESS_LIST}" "${MAIL_SUBJECT}" "`tail -100 ${LOG_FILE_PATH}`";
    exit 1;
fi

exit 0;
