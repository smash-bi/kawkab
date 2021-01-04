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
    conf['onEC2'] = True  # Set this to true when running experiments on EC2 cluster
    conf['use_alt_names'] = True  # Set this to true if mapping different colors, e.g., mapping
    # yellow to blue so that multiple super-leaves can be in the same rack in the SyN cluster.

    if conf['onEC2']:
        assert not conf['setup_ifaces']
        assert not conf['set_latencies']
        assert not conf['run_local']

    if conf['set_latencies']:
        assert conf['machines_group_indexing']

    # Configure the base IP of each color.
    conf['cluster_ips'] = {
                            #'red': '10.10.0',  # 10.10 or 10.64 or 10.0.1
                           # 'blue'  :'10.96.0',#'10.30 or 10.96 or 10.0.3
                           # 'yellow':'10.112.0',#10.40 or 10.112 or 10.0.4
                           # 'green' :'10.80.0',#10.420 or 10.112 or 10.0.4
                            'm1':'10.162.0',
                           }

    # Define the super-leaves. The color must be unique in all tuples.
    # The array in the value maps to the machine index in a SyN rack.
    # For example, ('red', [0, 1, 2]) means the machines red00, red01, red02.
    # The tuples and the number of machines in each tuple can be larger than
    # the super-leaves configuration that will be used in the experiment.
    conf['backend_indexes'] = [
                    #('red', [1, 2, 3]),
                    #('red', [2]),
                    ('m1', [7]),
    ]
    conf['server_indexes'] = [
                    #('red', [15, 13, 12, 14]),
                    #('red', [10, 11, 12, 13, 9]),
                    ('m1', [16]),
    ]

    conf['client_indexes'] = [
            ('m1', [
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
                6, 7, 27, 22, 6, 7, 27, 22,
            ]),
                    # ('red', [
                    #     3
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    #     # 5, 4, 6, 7, 8, 14, 15, 9,
                    # ]),
    ]

    if conf['onEC2']:  # If running experiments on EC2
        conf['root_dir'] = '/home/ubuntu/kawkab'
        conf['ssh_username'] = 'ubuntu'  # Remote ID to use for ssh connections
        #conf['rsaKeyFile'] = '/home/sm3rizvi/canopus/rsa-keys/aws-keypair'  # RSA keypair file for SSH connection
        conf['rsaKeyFile'] = None
        conf['kawkab_dir'] = '/home/ubuntu/kawkab'  # The directory where kawkab's code is stored. This directory should have src and build folders of the kawkab code.
        conf['allResultsDir'] = '/home/ubuntu/kawkab/experiments'  # Direcotry where results will be saved.
        conf['iface'] = 'eth0'  # Interface for communication between Canopus nodes
        conf['mavenRepos'] = '/home/ubuntu/.m2/repository'
    else:
        conf['root_dir'] = '/home/sm3rizvi/kawkab'
        conf['ssh_username'] = 'sm3rizvi'
        conf['rsaKeyFile'] = None  # None if RSA keypair file is not being used
        conf['kawkab_dir'] = '/home/sm3rizvi/kawkab'
        conf['iface'] = 'enp130s0'  # Interface for communication between Canopus nodes
        conf['mavenRepos'] = '/hdd1/sm3rizvi/maven-repos'

    conf['exp_base'] = 'results/kawkab'  # base directory to store results for all experiments
    conf['exp_dir'] = '%s/experiments/%s' % (
    conf['root_dir'], conf['exp_base'])  # actual directory to store the results from all experiments

    # --------------------------------------------------------
    # kawkab config
    # --------------------------------------------------------
    conf['server_base_port'] = 33433
    conf['client_base_port'] = 43567

    conf['batch_writeratio_rps'] = [  # list of tuples (batch_size, write_ratio, [reqs_per_second list])
        #(1000, 100, [10.25])
        #(5, 100, [1, 0.5, 1.25, 0.25, 1.5])
        #(10000, 100, [10, 9, 8])
        #(1, 1, [1])
        #(10000, 100, [5, 6, 7, 8, 9, 9.5])
        #(1000, 100, [8.5, 8.25, 8, 7.75, 7.5, 7.25, 7]),
        # 2 nodes -> 8.5, 8.25, 8
        # 3 nodes -> 8.5, 8.25, 8
        # 4 nodes -> 8.5, 8.25, 8
        # 5 nodes -> 7, 6.75, 6.5, 6.25, 7.5

        #(1000, 100, [8.25, 7.5, 7.25, 7, 7.75, 6.75]),
        # (1000, 100, [8.25, 7.5, 7.25, 7, 7.75, 6.75]),
        # (500, 100, [8.5, 8.25, 7.5, 7.25, 7, 7.75, 6.75]),
        # (100, 100, [8.5, 8.25, 7.5, 7.25, 7, 7.75, 6.75]),

        #(1000, 100, [11.5]),
        #(100, 100, [0.3, 0.5, 0.7, 1]),#, 14, 13, 14.75, 15]),
        #(100, 100, [0.5, 1, 2, 2.3]),#, 14, 13, 14.75, 15]),
        (1000, 80, [15]),#, 14, 13, 14.75, 15]),
    ]

    conf['test_type'] = ['rw']#['s3']
    conf['test_prefix'] = ['awshq6w']#['16kbs10']#['t1']#['16kbs5']#['hq21w']#['kw47']#['kw46']
    conf['record_size'] = [16]#[16]#[1, 512, 1024, 4096, 8192, 16384, 32768]#[1, 2, 4, 8, 16, 32, 64, 128]#[1, 2, 4, 8, 16]#[16]#[8]#[1024, 512]#[256]#[16]#[16]

    conf['files_per_client'] = [1]  # [2, 4, 12, 24, 36, 28]

    conf['clients_per_machine'] = [20]#[10]#[14, 13, 15]#10
    conf['num_client_procs'] = [30]#[40]#[80]#[80]#[40]#[20]#[10, 15, 40, 80, 5]
    conf['num_servers'] = [1]#[4]#[1,2,3,4,5]#[1]#[4,3]#[1]#[2, 3, 4]

    conf['test_duration'] = 180#700#60#600#1500#60
    conf['warmup_sec'] = 0#120#120#0#120
    conf['init_wait_msec'] = 0

    conf['test_runs'] = [1]

    conf['high_mps'] = 0#6
    conf['burst_dur_sec'] = 0#1
    conf['burst_prob_perc'] = 0#20
    conf['is_synchronous'] = "false"
    conf['read_recent'] = "true"

    conf['rpc_buf_len'] = 4*1024*1024#6*1024*1024

    conf['logGC'] = False
    conf[
        'server_jvm_params'] = '-ea -Xms12g -Xmx18g -XX:MaxDirectMemorySize=16684m -XX:+UnlockExperimentalVMOptions -XX:+UseZGC'
        #'server_jvm_params'] = '-ea -Xms4g -Xmx6g -XX:MaxDirectMemorySize=16684m -XX:+UnlockExperimentalVMOptions -XX:+UseZGC'
    conf['client_jvm_params'] = '-ea -XX:+UnlockExperimentalVMOptions -XX:+UseZGC'

    conf['server_classpath'] = ".:%s/src/main/resources:%s/target/classes:repoDir/com/google/guava/guava/28.1-jre/guava-28.1-jre.jar:repoDir/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:repoDir/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:repoDir/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar:repoDir/org/checkerframework/checker-qual/2.8.1/checker-qual-2.8.1.jar:repoDir/com/google/errorprone/error_prone_annotations/2.3.2/error_prone_annotations-2.3.2.jar:repoDir/com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar:repoDir/org/codehaus/mojo/animal-sniffer-annotations/1.17/animal-sniffer-annotations-1.17.jar:repoDir/net/openhft/chronicle-core/2.20.102/chronicle-core-2.20.102.jar:repoDir/com/intellij/annotations/12.0/annotations-12.0.jar:repoDir/net/openhft/chronicle-bytes/2.20.101/chronicle-bytes-2.20.101.jar:repoDir/net/openhft/chronicle-wire/2.20.101/chronicle-wire-2.20.101.jar:repoDir/net/openhft/compiler/2.3.4/compiler-2.3.4.jar:repoDir/net/openhft/chronicle-threads/2.20.100/chronicle-threads-2.20.100.jar:repoDir/net/openhft/affinity/3.1.11/affinity-3.1.11.jar:repoDir/commons-cli/commons-cli/1.4/commons-cli-1.4.jar:repoDir/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar:repoDir/net/openhft/chronicle-map/3.20.82/chronicle-map-3.20.82.jar:repoDir/net/openhft/chronicle-values/2.20.80/chronicle-values-2.20.80.jar:repoDir/com/squareup/javapoet/1.12.1/javapoet-1.12.1.jar:repoDir/net/openhft/chronicle-algorithms/2.20.80/chronicle-algorithms-2.20.80.jar:repoDir/net/java/dev/jna/jna/5.5.0/jna-5.5.0.jar:repoDir/net/java/dev/jna/jna-platform/5.5.0/jna-platform-5.5.0.jar:repoDir/com/thoughtworks/xstream/xstream/1.4.9/xstream-1.4.9.jar:repoDir/xmlpull/xmlpull/1.1.3.1/xmlpull-1.1.3.1.jar:repoDir/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar:repoDir/org/codehaus/jettison/jettison/1.3.8/jettison-1.3.8.jar:repoDir/stax/stax-api/1.0.1/stax-api-1.0.1.jar:repoDir/org/ops4j/pax/url/pax-url-aether/2.4.5/pax-url-aether-2.4.5.jar:repoDir/org/slf4j/jcl-over-slf4j/1.6.6/jcl-over-slf4j-1.6.6.jar:repoDir/org/agrona/agrona/1.0.0/agrona-1.0.0.jar:repoDir/org/apache/curator/curator-framework/4.2.0/curator-framework-4.2.0.jar:repoDir/org/apache/curator/curator-client/4.2.0/curator-client-4.2.0.jar:repoDir/org/apache/zookeeper/zookeeper/3.5.4-beta/zookeeper-3.5.4-beta.jar:repoDir/log4j/log4j/1.2.17/log4j-1.2.17.jar:repoDir/org/apache/yetus/audience-annotations/0.5.0/audience-annotations-0.5.0.jar:repoDir/io/netty/netty/3.10.6.Final/netty-3.10.6.Final.jar:repoDir/com/amazonaws/aws-java-sdk-s3/1.11.553/aws-java-sdk-s3-1.11.553.jar:repoDir/com/amazonaws/aws-java-sdk-kms/1.11.553/aws-java-sdk-kms-1.11.553.jar:repoDir/com/amazonaws/aws-java-sdk-core/1.11.553/aws-java-sdk-core-1.11.553.jar:repoDir/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar:repoDir/software/amazon/ion/ion-java/1.0.2/ion-java-1.0.2.jar:repoDir/com/fasterxml/jackson/core/jackson-databind/2.6.7.2/jackson-databind-2.6.7.2.jar:repoDir/com/fasterxml/jackson/core/jackson-annotations/2.6.0/jackson-annotations-2.6.0.jar:repoDir/com/fasterxml/jackson/core/jackson-core/2.6.7/jackson-core-2.6.7.jar:repoDir/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.6.7/jackson-dataformat-cbor-2.6.7.jar:repoDir/joda-time/joda-time/2.8.1/joda-time-2.8.1.jar:repoDir/com/amazonaws/jmespath-java/1.11.553/jmespath-java-1.11.553.jar:repoDir/javax/annotation/javax.annotation-api/1.2/javax.annotation-api-1.2.jar:repoDir/org/apache/thrift/libthrift/0.13.0/libthrift-0.13.0.jar:repoDir/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar:repoDir/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:repoDir/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar:repoDir/org/rocksdb/rocksdbjni/6.13.3/rocksdbjni-6.13.3.jar"%(conf['kawkab_dir'],conf['kawkab_dir'])
    conf['server_classpath'] = conf['server_classpath'].replace('repoDir', conf['mavenRepos'])
    conf['client_classpath'] = '.:%s/src/main/resources:%s/target/classes:${ld}/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar:${ld}/org/apache/thrift/libthrift/0.13.0/libthrift-0.13.0.jar:${ld}/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar:${ld}/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:${ld}/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar:${ld}/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar:${ld}/com/google/guava/guava/28.1-jre/guava-28.1-jre.jar:${ld}/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:${ld}/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:${ld}/com/amazonaws/aws-java-sdk-s3/1.11.553/aws-java-sdk-s3-1.11.553.jar:${ld}/com/amazonaws/aws-java-sdk-kms/1.11.553/aws-java-sdk-kms-1.11.553.jar:${ld}/com/amazonaws/aws-java-sdk-core/1.11.553/aws-java-sdk-core-1.11.553.jar:${ld}/software/amazon/ion/ion-java/1.0.2/ion-java-1.0.2.jar:${ld}/com/google/guava/guava/28.1-jre/guava-28.1-jre.jar:${ld}/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:${ld}/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:${ld}/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar:${ld}/org/checkerframework/checker-qual/2.8.1/checker-qual-2.8.1.jar:${ld}/com/google/errorprone/error_prone_annotations/2.3.2/error_prone_annotations-2.3.2.jar:${ld}/com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar:${ld}/org/codehaus/mojo/animal-sniffer-annotations/1.17/animal-sniffer-annotations-1.17.jar:${ld}/com/thoughtworks/xstream/xstream/1.4.9/xstream-1.4.9.jar:${ld}/xmlpull/xmlpull/1.1.3.1/xmlpull-1.1.3.1.jar:${ld}/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar:${ld}/org/codehaus/jettison/jettison/1.3.8/jettison-1.3.8.jar:${ld}/stax/stax-api/1.0.1/stax-api-1.0.1.jar:${ld}/org/ops4j/pax/url/pax-url-aether/2.4.5/pax-url-aether-2.4.5.jar:${ld}/org/slf4j/jcl-over-slf4j/1.6.6/jcl-over-slf4j-1.6.6.jar:${ld}/org/apache/curator/curator-framework/4.2.0/curator-framework-4.2.0.jar:${ld}/org/apache/curator/curator-client/4.2.0/curator-client-4.2.0.jar:${ld}/org/apache/zookeeper/zookeeper/3.5.4-beta/zookeeper-3.5.4-beta.jar:${ld}/log4j/log4j/1.2.17/log4j-1.2.17.jar:${ld}/org/apache/yetus/audience-annotations/0.5.0/audience-annotations-0.5.0.jar:${ld}/io/netty/netty/3.10.6.Final/netty-3.10.6.Final.jar:${ld}/com/amazonaws/aws-java-sdk-s3/1.11.553/aws-java-sdk-s3-1.11.553.jar:${ld}/com/amazonaws/aws-java-sdk-kms/1.11.553/aws-java-sdk-kms-1.11.553.jar:${ld}/com/amazonaws/aws-java-sdk-core/1.11.553/aws-java-sdk-core-1.11.553.jar:${ld}/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar:${ld}/software/amazon/ion/ion-java/1.0.2/ion-java-1.0.2.jar:${ld}/com/fasterxml/jackson/core/jackson-databind/2.6.7.2/jackson-databind-2.6.7.2.jar:${ld}/com/fasterxml/jackson/core/jackson-annotations/2.6.0/jackson-annotations-2.6.0.jar:${ld}/com/fasterxml/jackson/core/jackson-core/2.6.7/jackson-core-2.6.7.jar:${ld}/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.6.7/jackson-dataformat-cbor-2.6.7.jar:${ld}/joda-time/joda-time/2.8.1/joda-time-2.8.1.jar:${ld}/com/amazonaws/jmespath-java/1.11.553/jmespath-java-1.11.553.jar:${ld}/javax/annotation/javax.annotation-api/1.2/javax.annotation-api-1.2.jar:${ld}/org/apache/thrift/libthrift/0.13.0/libthrift-0.13.0.jar:${ld}/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar:${ld}/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:${ld}/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar'%(conf['kawkab_dir'],conf['kawkab_dir'])
    conf['client_classpath'] = conf['client_classpath'].replace('${ld}', conf['mavenRepos'])

    return conf
