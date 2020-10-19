def get_config():
    conf = {}

    conf['run_local'] = False  # Run on local machine. It would be better to not set this as true.
    # Instead, use local IP (127.0.0.1) as the machine IPs and set use_alt_names as true.

    conf[
        'setup_ifaces'] = False  # Changes the IP address of the p4p1/p6;1 interfaces so that machines can communicate across racks in SyN.

    conf['set_latencies'] = False  # Currently this will work if a super-leaf is within one rack (or color) only
    conf['tuneTCP'] = False  # Update buffer sizes for higher throughput
    conf[
        'machines_group_indexing'] = True  # Index machines in a group. Disables spanning a super-leaf across multiple racks.
    conf['onEC2'] = False  # Set this to true when running experiments on EC2 cluster
    conf['use_alt_names'] = True  # Set this to true if mapping different colors, e.g., mapping
    # yellow to blue so that multiple super-leaves can be in the same rack in the SyN cluster.

    if conf['onEC2']:
        assert not conf['setup_ifaces']
        assert not conf['set_latencies']
        assert not conf['run_local']

    if conf['set_latencies']:
        assert conf['machines_group_indexing']

    # Configure the base IP of each color.
    conf['cluster_ips'] = {'red': '10.10.0',  # 10.10 or 10.64 or 10.0.1
                           # 'blue'  :'10.96.0',#'10.30 or 10.96 or 10.0.3
                           # 'yellow':'10.112.0',#10.40 or 10.112 or 10.0.4
                           # 'green' :'10.80.0',#10.420 or 10.112 or 10.0.4
                           }

    # Define the super-leaves. The color must be unique in all tuples.
    # The array in the value maps to the machine index in a SyN rack.
    # For example, ('red', [0, 1, 2]) means the machines red00, red01, red02.
    # The tuples and the number of machines in each tuple can be larger than
    # the super-leaves configuration that will be used in the experiment.
    conf['backend_indexes'] = [
                    ('red', [1, 2, 3]),
                    # ('red',     [1,2,3]),
    ]
    conf['server_indexes'] = [
                    #('red', [15, 13, 12, 14]),
                    ('red', [15]),
    ]

    conf['client_indexes'] = [
                    # ('red', [
                    #     4, 5, 6, 7, 9, 10, 11, 8,
                    #     4, 5, 6, 7, 9, 10, 11, 8,
                    #     5, 6, 7, 8,
                    #     4, 5, 6, 7, 9, 10, 11, 8,
                    #     4, 5, 6, 7, 9, 10, 11, 8,
                    #     9, 10, 11, 8,
                    # ]),
                    ('red',     [4,5,6,7,8,
                                 4,5,6,7,8,
                                 4,5,6,7,8,
                                 4,5,6,7,8,
                                 ]),

    ]

    if conf['onEC2']:  # If running experiments on EC2
        conf['root_dir'] = '/home/ec2-user/kawkab'
        conf['ssh_username'] = 'ec2-user'  # Remote ID to use for ssh connections
        conf['rsaKeyFile'] = '/home/sm3rizvi/canopus/rsa-keys/aws-keypair'  # RSA keypair file for SSH connection
        conf[
            'kawkab_dir'] = '/home/ec2-user/kawkab'  # The directory where kawkab's code is stored. This directory should have src and build folders of the kawkab code.
        conf['allResultsDir'] = '/home/sm3rizvi/kawkab/experiments'  # Direcotry where results will be saved.
        conf['iface'] = 'eth0'  # Interface for communication between Canopus nodes
    else:
        conf['root_dir'] = '/home/sm3rizvi/kawkab'
        conf['ssh_username'] = 'sm3rizvi'
        conf['rsaKeyFile'] = None  # None if RSA keypair file is not being used
        conf['kawkab_dir'] = '/home/sm3rizvi/kawkab'
        conf['iface'] = 'enp130s0'  # Interface for communication between Canopus nodes

    conf['exp_base'] = 'results/kawkab'  # base directory to store results for all experiments
    conf['exp_dir'] = '%s/experiments/%s' % (
    conf['root_dir'], conf['exp_base'])  # actual directory to store the results from all experiments

    # --------------------------------------------------------
    # kawkab config
    # --------------------------------------------------------
    conf['server_base_port'] = 33433
    conf['client_base_port'] = 43567

    conf['batch_writeratio_rps'] = [  # list of tuples (batch_size, write_ratio, [reqs_per_second list])
        #(500, 100, [10.85]),
        #(20000, 100, [10.25]),
        #(30000, 100, [10.75]), #[12.25, 12, 11.75, 11.5, 11.25, 11, 10.75, 10.5, 10, 9, 8, 7]),
        #(20000, 100, [7,9, 9.5, 9.75, 10, 10.25, 10.5, 11, 11.5, 11.75, 12, 12.25, 12.5,12.75]),
        #(10000, 80, [10, 11, 12, 13, 13.5, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]),
        #(10000, 50, [2]), # [10, 13, 14, 15, 18.5, 19, 19.5]),#[17, 18, 19, 19.75, 20, 20.25, 20.5, 20.75, 21, 21.25, 21.5,]),
        #(10000, 20, [2]), #[41, 33, 30]),#[36, 38, 40, 42, 42.5, 43, 43.25, 43.5, 43.75, 44, 44.25])
        # (10000, 100,  [2]),#[10.35]),# [9.7, 10, 10.2, 10.3]),# [10, 10.2, 10.5, 10.75, 11, 9.75, 9.5, 9, 7, 11.25, 11.5]),
        # (10000, 80,  [2]),
        #(10000, 100, [9.5, 9.75, 10, 10.2, 10.5, 11, 11.25, 9, 7]),
        #(10000, 100, [4.5]),#[4.5]),

        #(1000, 80, [5 , 8, 10, 11, 12, 12.5, 12.75, 13, 13.25, 13.5, 13.75]),
        #(1000, 50, [10, 14, 16, 18.5, 18.75, 19, 19.25, 19.5, 20]),
        #(1000, 20, [30, 33, 41, 20, 36, 38, 39, 40, 41.25, 41.5, 42, 42.5])

        #(1000, 80, [5, 8, 10, 13, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]),
        #(1000, 50, [5, 10, 15, 16, 17, 18, 19, 20]),
        #(1000, 20, [5, 15, 25, 30, 32, 33, 34, 35, 35.5, 36, 36.5, 37, 38.5, 39]),

        #(10000, 100, [9])
        (1000, 100, [2, 4, 6, 8, 9, 10, 11, 12])

        #(1000, 100, [10.25])
    ]

    conf['test_type'] = ['rw']
    conf['test_prefix'] = ['kw48']#['hq21w']#['kw47']#['kw46']
    conf['record_size'] = [64, 128, 256]#[16]#[16]

    conf['clients_per_machine'] = 10
    conf['num_client_procs'] = [20]#[40]#[20]#[10, 15, 40, 80, 5]
    conf['num_servers'] = [1]#[4,3]#[1]#[2, 3, 4]

    conf['files_per_client'] = [1]  # [2, 4, 12, 24, 36, 28]

    conf['test_duration'] = 60#600#1500#60
    conf['warmup_sec'] = 120#0#120
    conf['init_wait_msec'] = 0

    conf['test_runs'] = [1]#[1, 2, 3, 4, 5]

    conf['high_mps'] = 0
    conf['burst_dur_sec'] = 0
    conf['burst_prob_perc'] = 20
    conf['is_synchronous'] = "false"
    conf['read_recent'] = "true"

    conf['logGC'] = False
    conf[
        'server_jvm_params'] = '-ea -Xms12g -Xmx24g -XX:MaxDirectMemorySize=16684m -XX:+UnlockExperimentalVMOptions -XX:+UseZGC'
        #'server_jvm_params'] = '-ea -Xms4g -Xmx6g -XX:MaxDirectMemorySize=16684m -XX:+UnlockExperimentalVMOptions -XX:+UseZGC'
    conf['client_jvm_params'] = '-ea -XX:+UnlockExperimentalVMOptions -XX:+UseZGC'

    conf[
        'server_classpath'] = "/home/sm3rizvi/kawkab/src/main/resources:/home/sm3rizvi/kawkab/target/classes:repoDir/com/google/guava/guava/28.1-jre/guava-28.1-jre.jar:repoDir/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:repoDir/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:repoDir/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar:repoDir/org/checkerframework/checker-qual/2.8.1/checker-qual-2.8.1.jar:repoDir/com/google/errorprone/error_prone_annotations/2.3.2/error_prone_annotations-2.3.2.jar:repoDir/com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar:repoDir/org/codehaus/mojo/animal-sniffer-annotations/1.17/animal-sniffer-annotations-1.17.jar:repoDir/net/openhft/chronicle-core/2.17.31/chronicle-core-2.17.31.jar:repoDir/com/intellij/annotations/12.0/annotations-12.0.jar:repoDir/net/openhft/chronicle-bytes/2.17.42/chronicle-bytes-2.17.42.jar:repoDir/net/openhft/chronicle-wire/2.17.59/chronicle-wire-2.17.59.jar:repoDir/net/openhft/compiler/2.3.4/compiler-2.3.4.jar:repoDir/net/openhft/chronicle-threads/2.17.18/chronicle-threads-2.17.18.jar:repoDir/net/openhft/affinity/3.1.11/affinity-3.1.11.jar:repoDir/commons-cli/commons-cli/1.4/commons-cli-1.4.jar:repoDir/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:repoDir/net/openhft/chronicle-map/3.17.8/chronicle-map-3.17.8.jar:repoDir/net/openhft/chronicle-values/2.17.2/chronicle-values-2.17.2.jar:repoDir/com/squareup/javapoet/1.5.1/javapoet-1.5.1.jar:repoDir/net/openhft/chronicle-algorithms/2.17.0/chronicle-algorithms-2.17.0.jar:repoDir/net/java/dev/jna/jna/4.2.1/jna-4.2.1.jar:repoDir/net/java/dev/jna/jna-platform/4.2.1/jna-platform-4.2.1.jar:repoDir/com/thoughtworks/xstream/xstream/1.4.9/xstream-1.4.9.jar:repoDir/xmlpull/xmlpull/1.1.3.1/xmlpull-1.1.3.1.jar:repoDir/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar:repoDir/org/codehaus/jettison/jettison/1.3.8/jettison-1.3.8.jar:repoDir/stax/stax-api/1.0.1/stax-api-1.0.1.jar:repoDir/org/ops4j/pax/url/pax-url-aether/2.4.5/pax-url-aether-2.4.5.jar:repoDir/org/slf4j/jcl-over-slf4j/1.6.6/jcl-over-slf4j-1.6.6.jar:repoDir/org/agrona/agrona/1.0.0/agrona-1.0.0.jar:repoDir/org/apache/curator/curator-framework/4.2.0/curator-framework-4.2.0.jar:repoDir/org/apache/curator/curator-client/4.2.0/curator-client-4.2.0.jar:repoDir/org/apache/zookeeper/zookeeper/3.5.4-beta/zookeeper-3.5.4-beta.jar:repoDir/log4j/log4j/1.2.17/log4j-1.2.17.jar:repoDir/org/apache/yetus/audience-annotations/0.5.0/audience-annotations-0.5.0.jar:repoDir/io/netty/netty/3.10.6.Final/netty-3.10.6.Final.jar:repoDir/com/amazonaws/aws-java-sdk-s3/1.11.553/aws-java-sdk-s3-1.11.553.jar:repoDir/com/amazonaws/aws-java-sdk-kms/1.11.553/aws-java-sdk-kms-1.11.553.jar:repoDir/com/amazonaws/aws-java-sdk-core/1.11.553/aws-java-sdk-core-1.11.553.jar:repoDir/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar:repoDir/software/amazon/ion/ion-java/1.0.2/ion-java-1.0.2.jar:repoDir/com/fasterxml/jackson/core/jackson-databind/2.6.7.2/jackson-databind-2.6.7.2.jar:repoDir/com/fasterxml/jackson/core/jackson-annotations/2.6.0/jackson-annotations-2.6.0.jar:repoDir/com/fasterxml/jackson/core/jackson-core/2.6.7/jackson-core-2.6.7.jar:repoDir/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.6.7/jackson-dataformat-cbor-2.6.7.jar:repoDir/joda-time/joda-time/2.8.1/joda-time-2.8.1.jar:repoDir/com/amazonaws/jmespath-java/1.11.553/jmespath-java-1.11.553.jar:repoDir/javax/annotation/javax.annotation-api/1.2/javax.annotation-api-1.2.jar:repoDir/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar:repoDir/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar:repoDir/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:repoDir/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar"
    conf['server_classpath'] = conf['server_classpath'].replace('repoDir', '/hdd1/sm3rizvi/maven-repos')
    conf[
        'client_classpath'] = '/home/sm3rizvi/kawkab/src/main/resources:/home/sm3rizvi/kawkab/target/classes:${ld}/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar:${ld}/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar:${ld}/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar:${ld}/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:${ld}/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar:${ld}/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:${ld}/com/google/guava/guava/28.1-jre/guava-28.1-jre.jar:${ld}/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:${ld}/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar'
    conf['client_classpath'] = conf['client_classpath'].replace('${ld}', '/hdd1/sm3rizvi/maven-repos')

    return conf
