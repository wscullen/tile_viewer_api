from collections import namedtuple
import logging
import os
from pathlib import Path
from subprocess import Popen, PIPE
import re
import shutil
import tarfile
import typing
import zipfile

module_logger = logging.getLogger("common.utils")

TaskStatus = namedtuple("TaskStatus", ["status", "message", "data"])


class ConfigFileProblem(Exception):
    pass


class ConfigValueMissing(Exception):
    pass


def unarchive(archive_path: Path, working_folder: Path) -> TaskStatus:
    """Unarchive the file if it a valid archive file (.zip or .tar.gz).

    Parameters
    ----------
    archive_path : Path
        Absolute path to archive file.

    working_folder : Path
        Absolute path to the directory the archive will be extracted to.

    Returns
    -------
    TaskStatus
        The TaskStatus namedtuple contains the status of the extraction. The
        product name is used as sanity check for data about the product name
        obtained through the USGS api. The TaskStatus contains "status", 
        a bool for extraction success, "message", a str for details on the extraction,
        and "data", an absolute path to the extracted product location.

    Notes
    -----
    Only .zip and .tar.gz archive types are supported currently.

    """

    module_logger.debug(f"archive_path {archive_path}")

    extract_status = None
    archive_path_ext = re.search("(.zip|.tar.gz)", str(archive_path))

    extraction_path = Path(working_folder)

    if archive_path_ext:
        product_name = ""
        if archive_path_ext.group(0) == ".zip":
            try:
                with zipfile.ZipFile(archive_path) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            product_name = Path(info.filename).parts[0]
                            break
                    else:
                        module_logger.error(
                            "Could not find a single directory in the file"
                        )
                        raise zipfile.BadZipFile

                    zf.extractall(path=extraction_path)

            except zipfile.BadZipFile as e:
                module_logger.error(
                    "Corrupted zip file, deleting, try the " "download again."
                )
                os.remove(archive_path)
                extract_status = TaskStatus(False, "Bad zip file", None)
            except BaseException as e:
                module_logger.error(
                    "Something blew up while unzip, deleting, try the download again."
                )
                module_logger.error(e)
                extract_status = TaskStatus(
                    False, "Generic problem while extracting zip", str(e)
                )

            else:

                extract_status = TaskStatus(
                    True, product_name, str(Path(extraction_path, product_name))
                )

        elif archive_path_ext.group(0) == ".tar.gz":
            module_logger.debug('Its a tar.gz')
            try:
                with tarfile.open(archive_path, "r:*") as tf:
                    module_logger.debug(tf.getnames())
                    for name in tf.getnames():
                        name_as_path = Path(name)
                        if name_as_path.suffix in ['.xml',".txt"]:
                            if name_as_path.suffix == '.xml':
                                product_name = name_as_path.stem
                            else:
                                product_name = name_as_path.stem[:-4]
                            
                            module_logger.debug(product_name)
                            break

                    tf.extractall(path=Path(extraction_path, product_name))

            except tarfile.TarError as e:
                module_logger.error(
                    "Corrupted tar file, deleting, try the " "download again."
                )
                module_logger.error(e)
                os.remove(archive_path)
                extract_status = TaskStatus(False, "Bad tar file", None)
            except BaseException as e:
                module_logger.error(
                    "Something blew up while unzip, deleting, try the download again."
                )
                module_logger.error(e)
                extract_status = TaskStatus(
                    False, "Generic problem while extracting tar", str(e)
                )

            else:

                extract_status = TaskStatus(
                    True, product_name, str(Path(extraction_path, product_name))
                )

    else:
        extract_status = TaskStatus(False, "Not a valid archive format", None)

    return extract_status


def clean_up_folder(working_folder: Path) -> None:
    """Clean up ALL left over files in specified folder.

    Parameters
    ----------
    working_folder : str
        Absolute path to archive file.

    Returns
    -------
    None

    Notes
    -----
    All files will be removed except the .gitignore file (used to keep the empty folder
    in the git history).

    """

    module_logger.warning(f"cleaning up folder {working_folder}")

    for root, dirs, files in os.walk(working_folder):

        for d in dirs:
            if d not in ["status", "input", "output"]:
                module_logger.info(f"Removing directory {str(Path(root, d))}")
                shutil.rmtree(Path(root, d))

        for f in files:
            if f != ".gitignore":
                module_logger.info(f"Removing file {str(Path(root, f))}")
                os.remove(Path(root, f))


def get_partial_path(full_path: Path, path_start: str) -> Path:
    """Given a full path, return a partial path where the path_start is the root

    Example:
    full path:
    /home/cullens/Development/simple_worker/working_folder/S1B_IW_GRDH_1SDV_20180403T002305_20180403T002330_010312_012C3E_DF82_NORLIM/Sigma0_VH.tif
    path_start:
    S1B_IW_GRDH_1SDV_20180403T002305_20180403T002330_010312_012C3E_DF82_NORLIM

    return:
    S1B_IW_GRDH_1SDV_20180403T002305_20180403T002330_010312_012C3E_DF82_NORLIM/Sigma0_VH.tif

    If path start does not match any parts of the path, retun None
    """

    path_parts = Path(full_path).parts
    path_start_search = [
        idx for idx, part in enumerate(path_parts) if part.startswith(str(path_start))
    ]

    if len(path_start_search) > 0:
        path_start_index = path_start_search[0]
    else:
        # Path segment not found, returning none
        return None

    path_product_only = Path(*path_parts[path_start_index:])

    return path_product_only


def mp_run(command):
    env_cur = {
        "HOSTNAME": "centos7-nfs-pod",
        "NGINX_PORT": "tcp://10.111.177.182:80",
        "NGINX_PORT_80_TCP_PORT": "80",
        "LESSOPEN": "||/usr/bin/lesspipe.sh %s",
        "KUBERNETES_PORT": "tcp://10.96.0.1:443",
        "PATH": "/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "KUBERNETES_SERVICE_PORT": "443",
        "LANG": "en_US.UTF-8",
        "TERM": "xterm",
        "SHLVL": "1",
        "KUBERNETES_SERVICE_HOST": "10.96.0.1",
        "NGINX_PORT_80_TCP_PROTO": "tcp",
        "LD_LIBRARY_PATH": ":/usr/local/lib",
        "HOME": "/root",
        "NGINX_PORT_80_TCP": "tcp://10.111.177.182:80",
        "KUBERNETES_PORT_443_TCP_ADDR": "10.96.0.1",
        "NGINX_SERVICE_HOST": "10.111.177.182",
        "QT_GRAPHICSSYSTEM_CHECKED": "1",
        "KUBERNETES_SERVICE_PORT_HTTPS": "443",
        "_": "/usr/bin/python",
        "KUBERNETES_PORT_443_TCP_PROTO": "tcp",
        "OLDPWD": "/",
        "KUBERNETES_PORT_443_TCP": "tcp://10.96.0.1:443",
        "NGINX_SERVICE_PORT": "80",
        "PWD": "/root/Development/simple_worker",
        "KUBERNETES_PORT_443_TCP_PORT": "443",
        "LS_COLORS": "rs=0:di=01;34:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:mi=01;05;37;41:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arc=01;31:*.arj=01;31:*.taz=01;31:*.lha=01;31:*.lz4=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.tzo=01;31:*.t7z=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.dz=01;31:*.gz=01;31:*.lrz=01;31:*.lz=01;31:*.lzo=01;31:*.xz=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.alz=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.cab=01;31:*.jpg=01;35:*.jpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.axv=01;35:*.anx=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=01;36:*.au=01;36:*.flac=01;36:*.mid=01;36:*.midi=01;36:*.mka=01;36:*.mp3=01;36:*.mpc=01;36:*.ogg=01;36:*.ra=01;36:*.wav=01;36:*.axa=01;36:*.oga=01;36:*.spx=01;36:*.xspf=01;36:",
        "NGINX_PORT_80_TCP_ADDR": "10.111.177.182",
    }

    logging.info(env_cur)
    env_cur[
        "PYTHONPATH"
    ] = "/usr/lib64/python27.zip:/usr/lib64/python2.7:/usr/lib64/python2.7/plat-linux2:/usr/lib64/python2.7/lib-tk:/usr/lib64/python2.7/lib-old:/usr/lib64/python2.7/lib-dynload:/usr/lib64/python2.7/site-packages:/usr/lib/python2.7/site-packages"
    env_cur["LD_LIBRARY_PATH"] = "$LD_LIBRARY_PATH:/usr/local/lib"
    env_cur["LANG"] = "en_US.UTF-8"

    with Popen(
        command, shell=True, stdout=PIPE, stderr=PIPE, bufsize=1, env=env_cur
    ) as sp:
        for line in sp.stdout:
            yield (line.decode("utf8").rstrip())


def run_cmd(cmd_string):
    logging.info(cmd_string)

    output = []
    errorOutput = []

    for line in mp_run(cmd_string):
        logging.info(line)
        output.append(line)
        if line.startswith("ERROR:"):
            errorOutput.append(line)

    logging.info("Done calling subprocess")

    if len(errorOutput) > 0:
        return TaskStatus(
            False, "Something went wrong trying to correct the product", errorOutput
        )

    return TaskStatus(True, "Product Corrected Successfully", None)


class TaskFailureException(Exception):
    pass
