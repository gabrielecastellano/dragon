#!/bin/bash

rm validation/*

sdo_number=3
neighbor_probability=20
node_number=1
bundle_percentage=40

agreement_timeout=3
weak_agreement_timeout=6

sdo_number=3
sed -i "7s/.*/    SDO_NUMBER = ${sdo_number}/" config/configuration.py
while [ ${sdo_number} -le 30 ]; do

    neighbor_probability=20
    min_probability=$((100-${sdo_number}*12))
    if [ ${sdo_number} -le 5 ] && [ ${neighbor_probability} -le ${min_probability} ]; then
        neighbor_probability=${min_probability}
    fi
    sed -i "9s/.*/    NEIGHBOR_PROBABILITY = ${neighbor_probability}/" config/configuration.py

    while [ ${neighbor_probability} -le 90 ]; do

        node_number=1
        sed -i "11s/.*/    NODE_NUMBER = ${node_number}/" config/configuration.py

        while [ ${node_number} -le 4 ]; do

            bundle_percentage=40
            sed -i "12s/.*/    BUNDLE_PERCENTAGE = ${bundle_percentage}/" config/configuration.py

            while [ ${bundle_percentage} -le 80 ]; do

                # increase timeout according with the problem size
                agreement_timeout=$(($((60*${bundle_percentage}/80 + 20*${sdo_number}/30 + 20*${node_number}/4))/10))
                weak_agreement_timeout=$((${agreement_timeout}*2))
                sed -i "3s/.*/    AGREEMENT_TIMEOUT = ${agreement_timeout}/" config/configuration.py
                sed -i "4s/.*/    WEAK_AGREEMENT_TIMEOUT = ${weak_agreement_timeout}/" config/configuration.py

                echo -e "Running agreement with "${sdo_number}" sdos, "${neighbor_probability}" neighbor_prob, "${node_number}" nodes "${bundle_percentage}" bundle_percentage ..."
                echo -e "AGREEMENT_TIMEOUT =  "${agreement_timeout}
                python3 test_script.py > "validation/"${sdo_number}"sdos__"${neighbor_probability}"neighbor_prob__"${node_number}"nodes__"${bundle_percentage}"bundle_percentage.txt"
                killall python3

                bundle_percentage=$((${bundle_percentage}+5))
                sed -i "12s/.*/    BUNDLE_PERCENTAGE = ${bundle_percentage}/" config/configuration.py
            done
            node_number=$((${node_number}+1))
            sed -i "11s/.*/    NODE_NUMBER = ${node_number}/" config/configuration.py
        done
        neighbor_probability=$((${neighbor_probability}+3))
        sed -i "9s/.*/    NEIGHBOR_PROBABILITY = ${neighbor_probability}/" config/configuration.py
    done
    sdo_number=$((${sdo_number}+1))
    sed -i "7s/.*/    SDO_NUMBER = ${sdo_number}/" config/configuration.py
done