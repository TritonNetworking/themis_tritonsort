#!/usr/bin/env python

import argparse, os, sys, paramiko, time
from plumbum.cmd import scp

from auth_utils import authenticate

INSTALL_FILES = {}
for provider in ["amazon", "google"]:
    INSTALL_FILES_DIR = os.path.join(
        os.path.dirname(__file__), provider, "image")

    REQUIRED_INSTALL_FILES = [
        "install_dependencies.sh", "configure_system.sh", "on_startup.sh",
        "motd-banner", "preinstall_themis.sh"]

    for install_file in REQUIRED_INSTALL_FILES:
        install_file = os.path.join(INSTALL_FILES_DIR, install_file)
        if not os.path.exists(install_file):
            print >>sys.stderr, "Could not find %s" % install_file
            sys.exit(1)

    INSTALL_FILES[provider] = [os.path.join(INSTALL_FILES_DIR, x) \
                     for x in os.listdir(INSTALL_FILES_DIR)]

BUFFER = 1048576

def run_remote_command(command, paramiko_ssh_client, streaming=False):
    channel = paramiko_ssh_client.get_transport().open_session()
    channel.get_pty()
    channel.exec_command(command)
    if streaming:
        while not channel.exit_status_ready():
            # Receive as much from stdout as we can
            while channel.recv_ready():
                stdout = channel.recv(BUFFER)
                print stdout,
            # Receive as much from stderr as we can
            while channel.recv_stderr_ready():
                stderr = channel.recv_stderr(BUFFER)
                print >>sys.stderr, stderr,
            # Sleep for a bit
            time.sleep(0.5)

        # Command has finished.
        return_code = channel.recv_exit_status()
        # Finish printing stdout and stderr
        stdout = channel.recv(BUFFER)
        while len(stdout) > 0:
            print stdout,
            stdout = channel.recv(BUFFER)
        stderr = channel.recv_stderr(BUFFER)
        while len(stderr) > 0:
            print stderr,
            stderr = channel.recv_stderr(BUFFER)

        if return_code != 0:
            print >>sys.stderr, "\nCommand '%s' failed with return code %d." % (
                command, return_code)
            sys.exit(return_code)
    else:
        return_code = channel.recv_exit_status()
        if return_code != 0:
            print >>sys.stderr, "Command '%s' failed with return code %d:\n" % (
                command, return_code)
            stderr = channel.recv_stderr(BUFFER)
            while len(stderr) > 0:
                print >>sys.stderr, stderr,
                stderr = channel.recv_stderr(BUFFER)

            sys.exit(return_code)

def install_themis(
        config, provider, remote_address, version, github_URL, github_key,
        skip_dependencies, skip_configuration):
    provider_info = authenticate(provider, config)
    private_key = provider_info["private_key"]
    remote_user = provider_info["remote_username"]

    # Open a connection to the remote VM.
    print "Connecting to %s..." % remote_address
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        remote_address, username=remote_user, key_filename=private_key)

    # Upload all install files with scp.
    run_remote_command("mkdir -p ~/install/", ssh_client)
    print "Uploading install files..."
    for install_file in INSTALL_FILES[provider]:
        scp["-i"][private_key][install_file]\
            ["%s@%s:~/install/" % (remote_user, remote_address)]()

    # Install dependencies for Themis
    if not skip_dependencies:
        print "Installing dependencies..."
        run_remote_command(
            "~/install/install_dependencies.sh", ssh_client, streaming=True)

    # Set up static system configuration for Themis
    if not skip_configuration:
        print "Configuring system..."
        run_remote_command(
            "~/install/configure_system.sh ~/install/on_startup.sh "
            "~/install/motd-banner %s %s" % (version, github_URL), ssh_client,
            streaming=True)

    if github_key != None:
        # User wants to pre-install Themis.
        # Upload key
        scp["-i"][private_key][github_key]\
            ["%s@%s:~/install/" % (remote_user, remote_address)]()
        key_basename = os.path.basename(github_key)

        # Preinstall themis using this key
        run_remote_command(
            "~/install/preinstall_themis.sh ~/install/%s" % key_basename,
            ssh_client, streaming=True)

    ssh_client.close()

def main():
    parser = argparse.ArgumentParser(
        description="Install Themis on a cloud provider.")
    parser.add_argument("config", help="Cloud provider config file")
    parser.add_argument(
        "provider", help="The provider to use (amazon or google)")

    parser.add_argument(
        "remote_address", help="The publicly accessible address of the VM")
    parser.add_argument(
        "version", help="Version number string for this Themis Image")
    parser.add_argument(
        "github_URL", help="Full URL of github repo")

    parser.add_argument(
        "--github_key", help="Private key file used to pre-install Themis from "
        "github")

    parser.add_argument(
        "--skip_dependencies", action="store_true", help="Don't install "
        "dependencies")
    parser.add_argument(
        "--skip_configuration", action="store_true", help="Don't configure "
        "Image for Themis")

    args = parser.parse_args()
    return install_themis(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
