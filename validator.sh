#!/bin/bash

rm validation/*

CONFIG_FILE=config/default-config.ini

sdo_number=3
neighbor_probability=20
node_number=1
bundle_percentage=40

agreement_timeout=3
weak_agreement_timeout=6

rm -r -f validation/
mkdir validation

sdo_number=3
sed -i "/sdo_number/c\sdo_number = ${sdo_number}" ${CONFIG_FILE}
while [ ${sdo_number} -le 30 ]; do

    neighbor_probability=20
    min_probability=$((100-${sdo_number}*12))
    if [ ${sdo_number} -le 5 ] && [ ${neighbor_probability} -le ${min_probability} ]; then
        neighbor_probability=${min_probability}
    fi
    sed -i "/neighbor_probability/c\neighbor_probability = ${neighbor_probability}" ${CONFIG_FILE}

    while [ ${neighbor_probability} -le 90 ]; do

        node_number=1
        sed -i "/node_number/c\node_number = ${node_number}" ${CONFIG_FILE}

        while [ ${node_number} -le 4 ]; do

            file_name="validation/"${sdo_number}"sdos__"${neighbor_probability}"neighbor_prob__"${node_number}"nodes.txt"
            echo "Output file: "${file_name}
            echo -e > ${file_name}
            echo ${sdo_number}" sdos, "${neighbor_probability}" neighbor_prob, "${node_number}" nodes " > ${file_name}

            bundle_percentage=40
            sed -i "/bundle_percentage/c\bundle_percentage = ${bundle_percentage}" ${CONFIG_FILE}

            while [ ${bundle_percentage} -le 80 ]; do

                # increase timeout according with the problem size
                agreement_timeout=$(($((50*${bundle_percentage}/80 + 30*${sdo_number}/30 + 20*${node_number}/4))/10))
                weak_agreement_timeout=$((${agreement_timeout}*2))
                sed -i "/\bagreement_timeout\b/c\agreement_timeout = ${agreement_timeout}" ${CONFIG_FILE}
                sed -i "/weak_agreement_timeout/c\weak_agreement_timeout = ${weak_agreement_timeout}" ${CONFIG_FILE}

                # fix sample frequency
                sample_frequency=$(echo "scale=3; (${agreement_timeout}/22)^3*5" | bc -l )
                sed -i "/sample_frequency/c\sample_frequency = ${sample_frequency}" ${CONFIG_FILE}

                # output some info
                echo -e "Running agreement with "${sdo_number}" sdos, "${neighbor_probability}" neighbor_prob, "${node_number}" nodes "${bundle_percentage}" bundle_percentage ..."
                echo -e "AGREEMENT_TIMEOUT = "${agreement_timeout}
                echo -e "SAMPLE_FREQUENCY = "${sample_frequency}

                # print info into log file
                echo "" >> ${file_name}
                echo "" >> ${file_name}
                echo "" >> ${file_name}
                echo "-----------------------------------------" >> ${file_name}
                echo "-" >> ${file_name}
                echo "SDO_NUMBER: "${sdo_number} >> ${file_name}
                echo "NEIGHBOR_PROBABILITY: "${neighbor_probability} >> ${file_name}
                echo "NODE_NUMBER: "${node_number} >> ${file_name}
                echo "BUNDLE_PERCENTAGE_LENGTH: "${bundle_percentage} >> ${file_name}
                echo "-" >> ${file_name}
                echo "AGREEMENT_TIMEOUT: "${agreement_timeout} >> ${file_name}
                echo "SAMPLE_FREQUENCY: "${sample_frequency} >> ${file_name}
                echo "-" >> ${file_name}

                # run the instance
                killall python3
                # python3 -m scripts.message_monitor &
                # monitor_pid=$!
                python3 test_script.py >> ${file_name}
                # kill -2 ${monitor_pid}
                # wait ${monitor_pid}
                killall python3
                python3 -m scripts.delete_queues
                # kill -9 ${monitor_pid}

                echo "-----------------------------------------" >> ${file_name}

                bundle_percentage=$((${bundle_percentage}+5))
                sed -i "/bundle_percentage/c\bundle_percentage = ${bundle_percentage}" ${CONFIG_FILE}
            done
            node_number=$((${node_number}+1))
            sed -i "/node_number/c\node_number = ${node_number}" ${CONFIG_FILE}
        done
        neighbor_probability=$((${neighbor_probability}+5))
        sed -i "/neighbor_probability/c\neighbor_probability = ${neighbor_probability}" ${CONFIG_FILE}
    done
    sdo_number=$((${sdo_number}+1))
    sed -i "/sdo_number/c\sdo_number = ${sdo_number}" ${CONFIG_FILE}
done