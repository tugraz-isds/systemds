#!/usr/bin/env bash
#-------------------------------------------------------------
#
# Modifications Copyright 2019 Graz University of Technology
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------

#function pause(){ read -p 'Press [Enter] key to continue...' }

#ToDo: Document simplified release script
function exit_with_usage {
  cat << EOF

Simplified release script. Documentation is yet to come ;-)

EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi


# Process each provided argument configuration
while [ "${1+defined}" ]; do
  IFS="=" read -ra PARTS <<< "$1"
  case "${PARTS[0]}" in
    --release-prepare)
      GOAL="release-prepare"
      RELEASE_PREPARE=true
      shift
      ;;
    --release-publish)
      GOAL="release-publish"
      RELEASE_PUBLISH=true
      shift
      ;;
    --release-snapshot)
      GOAL="release-snapshot"
      RELEASE_SNAPSHOT=true
      shift
      ;;
    --gitCommitHash)
      GIT_REF="${PARTS[1]}"
      shift
      ;;
    --gitTag)
      GIT_TAG="${PARTS[1]}"
      shift
      ;;
    --releaseVersion)
      RELEASE_VERSION="${PARTS[1]}"
      shift
      ;;
    --developmentVersion)
      DEVELOPMENT_VERSION="${PARTS[1]}"
      shift
      ;;
    --releaseRc)
      RELEASE_RC="${PARTS[1]}"
      shift
      ;;
    --tag)
      RELEASE_TAG="${PARTS[1]}"
      shift
      ;;
    --dryRun)
      DRY_RUN="-DdryRun=true"
      shift
      ;;

    *help* | -h)
      exit_with_usage
     exit 0
     ;;
    -*)
     echo "Error: Unknown option: $1" >&2
     exit 1
     ;;
    *)  # No more options
     break
     ;;
  esac
done


if [[ -z "$GPG_PASSPHRASE" ]]; then
    echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
    echo 'unlock the GPG signing key that will be used to sign the release!'
    echo
    stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
fi

if [[ -z "$GPG_KEYID" ]]; then
    echo 'The environment variable GPG_KEYID is not set. Enter the key ID for the '
    echo ' GPG signing key that will be used to sign the release!'
    echo
    stty -echo && printf "GPG key ID: " && read GPG_KEYID && printf '\n' && stty echo
fi

if [[ "$RELEASE_PREPARE" == "true" && -z "$RELEASE_VERSION" ]]; then
    echo "ERROR: --releaseVersion must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ "$RELEASE_PREPARE" == "true" && -z "$DEVELOPMENT_VERSION" ]]; then
    echo "ERROR: --developmentVersion must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ "$RELEASE_PUBLISH" == "true"  ]]; then
    if [[ "$GIT_REF" && "$GIT_TAG" ]]; then
        echo "ERROR: Only one argumented permitted when publishing : --gitCommitHash or --gitTag"
        exit_with_usage
    fi
    if [[ -z "$GIT_REF" && -z "$GIT_TAG" ]]; then
        echo "ERROR: --gitCommitHash OR --gitTag must be passed as an argument to run this script"
        exit_with_usage
    fi
fi

if [[ "$RELEASE_PUBLISH" == "true" && "$DRY_RUN" ]]; then
    echo "ERROR: --dryRun not supported for --release-publish"
    exit_with_usage
fi

if [[ "$RELEASE_SNAPSHOT" == "true" && "$DRY_RUN" ]]; then
    echo "ERROR: --dryRun not supported for --release-publish"
    exit_with_usage
fi

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}
if [[ "$RELEASE_PUBLISH" == "true" && "$GIT_TAG" ]]; then
    GIT_REF="tags/$GIT_TAG"
fi

BASE_DIR=$(pwd)
RELEASE_WORK_DIR=$BASE_DIR/target/release

# used for debugging build scripts:
#MVN="mvn --batch-mode --debug --errors"
MVN="mvn"

PUBLISH_PROFILES="-Pdistribution,rat"

if [ -z "$RELEASE_RC" ]; then
  RELEASE_RC="rc1"
fi

if [ -z "$RELEASE_TAG" ]; then
  RELEASE_TAG="v$RELEASE_VERSION-$RELEASE_RC"
fi

RELEASE_STAGING_LOCATION="${SYSTEMDS_ROOT}/temp"


echo "  "
echo "-------------------------------------------------------------"
echo "------- Release preparation with the following parameters ---"
echo "-------------------------------------------------------------"
echo "Executing           ==> $GOAL"
echo "Git reference       ==> $GIT_REF"
echo "release version     ==> $RELEASE_VERSION"
echo "development version ==> $DEVELOPMENT_VERSION"

echo "  "
echo "Deploying to :"
echo $RELEASE_STAGING_LOCATION
echo "  "

function checkout_code {
    # Checkout code
    rm -rf $RELEASE_WORK_DIR
    mkdir -p $RELEASE_WORK_DIR
    cd $RELEASE_WORK_DIR
    git clone https://github.com/tugraz-isds/systemds.git
    cd systemds
    git checkout $GIT_REF
    git_hash=`git rev-parse --short HEAD`
    echo "Checked out SystemDS git hash $git_hash"

    git clean -d -f -x
    #rm .gitignore
    #rm -rf .git

    cd "$BASE_DIR" #return to base dir
}

if [[ "$RELEASE_PUBLISH" == "true" ]]; then
    echo "Preparing release $RELEASE_VERSION"
    checkout_code
    cd $RELEASE_WORK_DIR/systemds

    $MVN clean package verify install:install deploy:deploy -DaltDeploymentRepository=altDepRepo::default::file://$SYSTEMDS_ROOT/temp -DskiptTests -Dgpg.keyname=$GPG_KEYID -Dgpg.passphrase=$GPG_PASSPHRASE \
    -Darguments="-DskipTests -DaltDeploymentRepository='altDepRepo::default::file://$SYSTEMDS_ROOT/temp' -Dgpg.keyname='$GPG_KEYID' -Dgpg.passphrase='$GPG_PASSPHRASE'" $PUBLISH_PROFILES | tee $BASE_DIR/publish-output.log

    cd "$BASE_DIR" #exit target

    exit 0
fi

cd "$BASE_DIR" #return to base dir
echo "ERROR: wrong execution goals"
exit_with_usage
