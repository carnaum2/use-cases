# coding=utf-8
import subprocess
import os
import sys


def save_df_to_hdfs(df, path_dir):
    df.write.mode('overwrite').format('parquet').save(path_dir)

def save_df_to_hdfs_csv(df, path_dir):
    df.write.mode('overwrite').format('csv').option('sep', '|').option('header', 'true').save(path_dir)

def save_df(df, path_filename, csv=True):
    import os
    dir_name, file_name = os.path.split(path_filename)
    ret_stat = create_directory(os.path.join(dir_name))
    print("Created directory returned {}".format(ret_stat))
    print("Started to save...")
    if csv:
        save_df_to_hdfs_csv(df, path_filename)
    else:
        save_df_to_hdfs(df, path_filename)
    print("Saved df successfully - '{}'".format(path_filename))


def get_name_nodes():
    out = subprocess.check_output('hdfs getconf -namenodes', shell=True)
    nodes = ['hdfs://'+n for n in out.strip().split(' ')]
    return nodes



def check_hdfs_exists(path_to_file):
    try:
        cmd = 'hdfs dfs -ls {}'.format(path_to_file).split()
        files = subprocess.check_output(cmd).strip().split('\n')
        return True if files and len(files[0])>0 else False
    except:
        return False

def check_local_exists(path_to_file):
    if not os.path.exists(path_to_file):
        print("'{}' no existe".format(path_to_file))
        return False
    else:
        return True

def run_cmd(args_list):
    """
    run linux commands
    """
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def move_hdfs_dir_to_local(hdfs_dir, local_dir):

    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-copyToLocal', hdfs_dir, local_dir])
    return ret

def mv_dir(hdfs_dir_src, hdfs_dir_dest):

    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-mv', hdfs_dir_src, hdfs_dir_dest])
    return ret


# https://community.hortonworks.com/articles/92321/interacting-with-hadoop-hdfs-using-python-codes.html
def move_hdfs_file_to_local(hdfs_dir, local_dir):
    '''
    Move the file contained in hdfs_dir to local_dir. If there are more than one file, returns an error
    :param hdfs_dir:
    :param local_dir:
    :return:
    '''
    print(local_dir)
    # So far, this method expects only one csv in 'hdfs_file_path'
    directory_list = subprocess.check_output(["hadoop", "fs", "-ls", hdfs_dir]).split("\n")
    files_list = [item.split(" ")[-1] for item in directory_list if not item.startswith("Found") and len(item) > 0
                                                                    and item.endswith(".csv")]
    if len(files_list)>1:
        logger.error("Not sure how to deal with more than one csv...") #TODO
        sys.exit(1)


    hdfs_filename = files_list[0]
    local_filename = os.path.join(local_dir, os.path.basename(files_list[0]))

    logger.info("Moving '{}' to '{}'".format(hdfs_filename, local_filename))

    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-get', hdfs_filename, local_filename])
    return local_filename

def move_local_file_to_hdfs(hdfs_dir, local_filename):
    if logger: logger.info("Moving '{}' to hdfs '{}'".format(local_filename, hdfs_dir))
    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', local_filename, hdfs_dir])
    return None

def create_directory(hdfs_dir):
    if not check_hdfs_exists(hdfs_dir):
        print("Directory '{}' does not exist. Creating...".format(hdfs_dir))
        cmd = 'hdfs dfs -mkdir -p {}'.format(hdfs_dir).split()
        return_state = subprocess.check_output(cmd)
        print("create_directory return_state='{}'".format(return_state))

        return True if return_state == "" else False
    return False

def get_merge(hdfs_dir, local_file, pattern=None):
    '''

    :param hdfs_dir:
    :param local_file:
    :param pattern: Ex. *.csv
    :return:
    '''

    hdfs_dir = '{}/'.format(hdfs_dir) if not pattern else '{}/{}'.format(hdfs_dir, pattern)
    print("Merging directory '{}' to local_file '{}'".format(hdfs_dir, local_file))
    cmd = 'hdfs dfs -getmerge {} {}'.format(hdfs_dir, local_file)
    print("CMD '{}'".format(cmd))
    (ret, out, err) = run_cmd(cmd.split(' '))
    #return_state = subprocess.check_output(cmd)

    # Remove headers - every part has the header, and it is replicated in the merge
    print("Removing duplicated headers")
    cmd = "head -1 {}".format(local_file)
    (ret, header_line, err) = run_cmd(cmd.split(' '))
    header_1st_word = header_line.split("|")[0]
    import subprocess
    if sys.platform == "darwin": #MacOS
        subprocess.call(["sed -i '' -e '1!{/^" + header_1st_word + "/d;}' " + local_file], shell=True)
    else:
        subprocess.call(["sed -i -e '1!{/^" + header_1st_word + "/d;}' " + local_file], shell=True)

    return ret


def get_merge_hdfs(hdfs_dir, local_file, pattern, hdfs_dir_dest=None):
    '''
    Merge a directory of files with pattern (e.g. *.csv) in only one file named 'local_file'
    The 'local_file' is moved to hdfs again. If hdfs_dir_dest is not None, use it. If it is None, use hdfs_dir
    :param hdfs_dir:
    :param local_file:
    :param pattern:
    :param hdfs_dir_dest:
    :return:
    '''
    get_merge(hdfs_dir, local_file, pattern)
    hdfs_dir_dest = hdfs_dir if not hdfs_dir_dest else hdfs_dir_dest
    move_local_file_to_hdfs(hdfs_dir_dest, local_file)
