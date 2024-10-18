#
#
#  Copyright 2024 CUBRID Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

if [ $# -ne 0 ];then
  curr=$(pwd)
  cd $1

  if [ $? -ne 0 ];then
    echo "$1: no such directory or permission denied"
    exit
  fi

  api_broker_home=$(pwd)
  cd $curr
else
  api_broker_home=$(pwd)
fi

echo "Is the API Broker installed in "$api_broker_home" ? [Yn]:"

read line leftover
is_installed_dir=TRUE

case ${line} in
  n* | N*)
    is_installed_dir=FALSE
esac

if [ "x${is_installed_dir}x" = "xFALSEx" ];then
  echo ""
  echo "Please enter the directory where API Broker is installed: "

  read input_dir leftover
  api_broker_home=${input_dir}
fi

if [ ! -d $api_broker_home ];then
  echo "$api_broker_home: no such directory"
  exit
fi

# environment variables for *csh
api_broker_csh_envfile="$HOME/.api-broker.csh"
cp ${api_broker_home}/share/scripts/api-broker.csh ${api_broker_csh_envfile}_temp
sed -i '/setenv API /d' ${api_broker_csh_envfile}_temp
sed -i "/API_DATABASES/isetenv API $api_broker_home" ${api_broker_csh_envfile}_temp

# environment variables for *sh
api_broker_sh_envfile="$HOME/.api-broker.sh"
cp ${api_broker_home}/share/scripts/api-broker.sh ${api_broker_sh_envfile}_temp
sed -i "/API=/d" ${api_broker_sh_envfile}_temp
sed -i "/API_DATABASE/iexport API=$api_broker_home" ${api_broker_sh_envfile}_temp

# environment variables for *sh
echo ""
for e in "$api_broker_csh_envfile" "$api_broker_sh_envfile"; do
  if [ -r "${e}" ]; then
    echo "Copying old ${e} to ${e}.bak ..."
    mv -f "${e}" "${e}.bak"
  fi
  mv "${e}_temp" "${e}"
done

# append script for executing .cubrid.sh to .bash_profile
PRODUCT_NAME="API-BROKER"
API_BROKER_SH_INSTALLED=1
if [ -z "$SHELL" ];then
   if [ ! -r /etc/passwd ];then
      user_sh="bash"
   else
      user_name=$(id -nu)
      user_sh=$(egrep -w "^$user_name" /etc/passwd | cut -d':' -f7-7)
      user_sh=${user_sh:-none}
      user_sh=$(basename $user_sh)
   fi
else
  user_sh=$(basename $SHELL)
fi

bash_exist=1
case $user_sh in
	zsh)
		sh_profile=$HOME/.zshrc
		;;
	bash)
		sh_profile=$HOME/.bash_profile
		;;
	sh)
		sh_profile=$HOME/.profile
		;;
	csh | tcsh)
		sh_profile=$HOME/.cshrc
		;;
	*)
		# if $sh_profile is null install script will stop following grep
		echo "$user_sh: unknown SHELL, force set to /bin/bash"
		sh_profile=$HOME/.bash_profile
		API_BROKER_SH_INSTALLED=0
		;;
esac

if [ ! -f $sh_profile ];then
  bash_exist=0
  touch $sh_profile
fi

append_profile=$(grep "${PRODUCT_NAME} environment" $sh_profile)

if [ -z "$append_profile" ];then
  echo '#-------------------------------------------------------------------------------' >> $sh_profile
  if [ $? -ne 0 ];then
    API_BROKER_SH_INSTALLED=0
    echo "Please check your permission for file $sh_profile"
  else
    echo '# set '${PRODUCT_NAME}' environment variables'                                    >> $sh_profile
    echo '#-------------------------------------------------------------------------------' >> $sh_profile

    case $user_sh in
      bash | sh)
        echo 'if [ -f $HOME/.api-broker.sh ];then'                                              >> $sh_profile
        echo '. $HOME/.api-broker.sh'                                                           >> $sh_profile
        echo 'fi'                                                                           >> $sh_profile
        ;;
      csh | tcsh)
        echo 'if ( -f ~/.api-broker.csh ) then' 		>> $sh_profile
        echo '  source ~/.api-broker.csh' 			>> $sh_profile
        echo 'endif' 					>> $sh_profile
        ;;
      zsh)
        echo 'if [ -f $HOME/.api-broker.sh ];then'                                              >> $sh_profile
        echo '  source $HOME/.api-broker.sh'                                                    >> $sh_profile
        echo 'fi'                                                                           >> $sh_profile
        ;;
      *)
        API_BROKER_SH_INSTALLED=0
        ;;
    esac
  fi	# $? - ne 0
fi	# -z "$append_profile"

if [ $API_BROKER_SH_INSTALLED -eq 1 ] && [ $bash_exist -eq 0 ];then
  echo "Notification: $sh_profile is created"
fi

echo ""
echo "If you want to use API Broker, run the following command to set required environment variables."
if [ $API_BROKER_SH_INSTALLED -eq 0 ];then
        echo "(or you can add the command into your current shell profile file to set permanently)"
        exit
fi
case "$SHELL" in
  */csh | */tcsh )
    echo "  $ source $api_broker_csh_envfile"
    ;;
  *)
    echo "  $ . $api_broker_sh_envfile"
    ;;
esac
echo ""

exit 0
