import os, ConfigParser, plumbum

def authenticate(provider, config_file):
    parser = ConfigParser.SafeConfigParser()
    parser.read(config_file)

    if not parser.has_section(provider):
        return None

    provider_info = {}

    if provider == "amazon":
        aws_access_key_id = parser.get("amazon", "aws_access_key_id")
        aws_secret_access_key = parser.get("amazon", "aws_secret_access_key")
        region = parser.get("amazon", "region")
        private_key= parser.get("amazon", "private_key")
        remote_username = parser.get("amazon", "remote_username")

        print "Configuring amazon credentials..."
        amazon_dir = os.path.expanduser("~/.aws/")
        if not os.path.exists(amazon_dir):
            os.mkdir(amazon_dir)

        # Load the amazon config file if it exists.
        aws_parser = ConfigParser.SafeConfigParser()
        aws_config_path = os.path.join(amazon_dir, "config")
        if os.path.exists(aws_config_path):
            aws_parser.read(aws_config_path)

        # Overwrite the themis section of the config file with new key
        # information.
        aws_parser.remove_section("profile themis")
        aws_parser.add_section("profile themis")
        aws_parser.set("profile themis", "region", region)
        aws_parser.set("profile themis", "aws_access_key_id", aws_access_key_id)
        aws_parser.set(
            "profile themis", "aws_secret_access_key", aws_secret_access_key)
        with open(aws_config_path, "w") as fp:
            aws_parser.write(fp)

        # Test that aws CLI works by listing your root S3 directory
        aws = plumbum.local["aws"]
        aws["--profile"]["themis"]["s3"]["ls"]()

        provider_info["aws_access_key_id"] = aws_access_key_id
        provider_info["aws_secret_access_key"] = aws_secret_access_key

        provider_info["region"] = region
        provider_info["private_key"] = private_key
        provider_info["remote_username"] = remote_username
    elif provider == "google":
        skip_authentication = bool(
            parser.get("google", "skip_authentication"))
        project = parser.get("google", "project")
        private_key = parser.get("google", "private_key")
        remote_username = parser.get("google", "remote_username")

        gcloud = plumbum.local["gcloud"]
        if not skip_authentication:
            gcloud["auth"]["login"]()
        gcloud["config"]["set"]["project"][project]()

        provider_info["project"] = project

        provider_info["private_key"] = private_key
        provider_info["remote_username"] = remote_username
    else:
        raise ValueError("Unknown provider %s. Valid providers are amazon and "\
                         "google" % provider)

    return provider_info
