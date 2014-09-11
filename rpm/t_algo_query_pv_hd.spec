#########################################################################
#help from to http://twiki.corp.alimama.com/twiki/bin/view/Alimm_OPS/RPM#
#########################################################################
Name: t_algo_query_pv_hd
Version: %{_version}
Release: %(echo $RELEASE)%{?dist}
Summary: query_pv hadoop
URL: http://yum.corp.alimama.com
Group: Algo
Packager: jianshen
License: GPL
#Copyright: Taobao
BuildArchitectures: noarch
BuildRoot: /var/tmp/%{name}-%{version}-%{release}
Requires: t_algo_db_common,sds_parser_p4ppvlogparser,algo1_hadoop_tools,sds_share_tools,sds_share_hadoopframework
%define _builddir .
%define _rpmdir .
%define ins_dir /home/a/share/phoenix/algo_db/hadoop/query_pv
%description
t_algo_query_pv_hd
%{_svn_path}
%{_svn_revision}

%prep
%build

%install
echo ${RPM_BUILD_ROOT}

rm -rf ${RPM_BUILD_ROOT}

mkdir -p ${RPM_BUILD_ROOT}
mkdir -p ${RPM_BUILD_ROOT}%ins_dir
mkdir -p ${RPM_BUILD_ROOT}%ins_dir/script
mkdir -p ${RPM_BUILD_ROOT}%ins_dir/conf
mkdir -p ${RPM_BUILD_ROOT}%ins_dir/log
mkdir -p ${RPM_BUILD_ROOT}%ins_dir/jar
mkdir -p ${RPM_BUILD_ROOT}%ins_dir/data

cp ../script/* ${RPM_BUILD_ROOT}%ins_dir/script
cp ../conf/*.conf ${RPM_BUILD_ROOT}%ins_dir/conf
cp ../jar/*.jar ${RPM_BUILD_ROOT}%ins_dir/jar

%pre
%post

%preun

%postun

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0755,ads,users)
%dir %ins_dir
%dir %ins_dir/script
%dir %ins_dir/conf
%dir %ins_dir/log
%dir %ins_dir/jar
%dir %ins_dir/data

%defattr(0755,ads,users)
%{ins_dir}/jar/*.jar
%{ins_dir}/script/*
%{ins_dir}/conf/*.conf
%config %ins_dir/conf/*.conf

%changelog
* Mon Dec 28 2009 jianshen <taobao-ad-research@list.alibaba-inc.com> -0.0.1-1
- Rebuilt for Red Hat Enterprise Linux AS release 4 (Nahant Update 7) 
