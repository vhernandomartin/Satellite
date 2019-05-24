#!/usr/bin/python
#
# DESCRIPTION: Script to get a report of pending & applied ERRATAS on hosts managed by Satellite.
# Please notice that is necessary to set up your candlepin and foreman passwords.
# candlepin_db_password = $(grep jpa.config.hibernate.connection.password /etc/candlepin/candlepin.conf | cut -d "=" -f 2)
# foreman_db_password = $(grep password /etc/foreman/database.yml|awk '{print $2}'|sed 's/"//g')
#
# DISCLAIMER: Use it at your own risk, this is not an oficial Satellite script and no support will be provided.
#
# AUTHOR: Victor Hernando
# DATE: 2019-05-21
#

import psycopg2
import csv

####### GLOBAL VARIABLES ########
candlepin_db_user = 'candlepin'
candlepin_db_password = <candlepin_password>
candlepin_db_name = 'candlepin'
foreman_db_user = 'foreman'
foreman_db_password = <foreman_password>
foreman_db_name = 'foreman'
posgtresql_hostname = 'localhost'
HostErratas = []
####### END GLOBAL VARIABLES ########

def connect(user,password,dbname,host):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(user = user, password = password, database = dbname, host = host )

        # create a cursor
        cur = conn.cursor()
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def get_hosts(cur):
    get_hosts_sql = "select name from cp_consumer"
    cur.execute(get_hosts_sql)

    # display all content HOSTS
    hosts_list = cur.fetchall()
    return hosts_list

def get_erratas(hosts_list,cur,cur2):
    for consumer_hosts in hosts_list:
        try:
            #print '####' + consumer_hosts[0] + '####'
            #print('Installed Erratas:')
            get_installed_errata_sql = "select b.name, \
                                           a.element, \
                                           a.mapkey \
                                    from   cp_consumer_facts a,cp_consumer b \
                                    where  a.cp_consumer_id=b.id \
                                    and    b.name = %s \
                                    and    a.mapkey like \'errata.count%%\'"
            cur.execute(get_installed_errata_sql, (consumer_hosts[0],))
            installed_erratas_summary = cur.fetchall()
            #print(installed_erratas_summary)

            hostname                        = consumer_hosts[0]
            installed_important             = installed_erratas_summary[0][1]
            installed_low                   = installed_erratas_summary[1][1]
            installed_moderate              = installed_erratas_summary[2][1]
            installed_critical              = installed_erratas_summary[3][1]
            installed_bugfix                = installed_erratas_summary[4][1]
            installed_total                 = installed_erratas_summary[5][1]
            installed_security              = installed_erratas_summary[6][1]
            installed_enhancement           = installed_erratas_summary[7][1]

        except:
            hostname                        = consumer_hosts[0]
            installed_important             = 0
            installed_low                   = 0
            installed_moderate              = 0
            installed_critical              = 0
            installed_bugfix                = 0
            installed_total                 = 0
            installed_security              = 0
            installed_enhancement           = 0

        try:
            get_pending_errata_sql = "select    b.name, \
                                    a.installable_security_errata_count, \
                                    a.installable_enhancement_errata_count, \
                                    a.installable_bugfix_errata_count, \
                                    a.applicable_rpm_count, \
                                    a.upgradable_rpm_count, \
                                    (a.installable_security_errata_count+a.installable_enhancement_errata_count+a.installable_bugfix_errata_count) TOTAL_PENDING \
                          from      katello_content_facets a,hosts b \
                          where     a.host_id=b.id \
                          and       b.name = %s"
            cur2.execute(get_pending_errata_sql, (consumer_hosts[0],))
            pending_erratas_summary = cur2.fetchall()
            #print(installed_erratas_summary)
            get_pending_30_days_sql =   "select count(*) CRITICAL_PENDING_30_DAYS \
                                        from    katello_content_facets a, \
                                                katello_content_facet_errata b, \
                                                katello_errata c, \
                                                hosts d \
                                        where   b.content_facet_id = a.id \
                                        and     b.erratum_id = c.id \
                                        and     a.host_id = d.id \
                                        and     d.name = %s \
                                        and     c.errata_type='security' \
                                        and     c.severity='Critical' \
                                        and     c.issued < (current_date-30)"
            cur2.execute(get_pending_30_days_sql, (consumer_hosts[0],))
            pending_30_days_count = cur2.fetchall()

            installable_sec_errata_count    = pending_erratas_summary[0][1]
            installable_enh_errata_count    = pending_erratas_summary[0][2]
            installable_bug_errata_count    = pending_erratas_summary[0][3]
            applicable_rpm_count            = pending_erratas_summary[0][4]
            upgradable_rpm_count            = pending_erratas_summary[0][5]
            installable_pending             = pending_erratas_summary[0][6]
        except:
            installable_sec_errata_count    = 0
            installable_enh_errata_count    = 0
            installable_bug_errata_count    = 0
            applicable_rpm_count            = 0
            upgradable_rpm_count            = 0
            installable_pending             = 0

        try:
            get_pending_30_days_sql =   "select count(*) CRITICAL_PENDING_30_DAYS \
                                        from    katello_content_facets a, \
                                                katello_content_facet_errata b, \
                                                katello_errata c, \
                                                hosts d \
                                        where   b.content_facet_id = a.id \
                                        and     b.erratum_id = c.id \
                                        and     a.host_id = d.id \
                                        and     d.name = %s \
                                        and     c.errata_type='security' \
                                        and     c.severity='Critical' \
                                        and     c.issued < (current_date-30)"
            cur2.execute(get_pending_30_days_sql, (consumer_hosts[0],))
            pending_30_days_count = cur2.fetchall()

            critical_pending_30_days        = pending_30_days_count[0][0]
        except:
            critical_pending_30_days        = pending_30_days_count[0][0]

        errata_line = ("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s\n" % (hostname,
                                                installed_important,
                                                installed_low,
                                                installed_moderate,
                                                installed_critical,
                                                installed_bugfix,
                                                installed_total,
                                                installed_security,
                                                installed_enhancement,
                                                installable_sec_errata_count,
                                                installable_enh_errata_count,
                                                installable_bug_errata_count,
                                                applicable_rpm_count,
                                                upgradable_rpm_count,
                                                installable_pending,
                                                critical_pending_30_days))
        HostErratas.append(errata_line)

def write_errata_to_file():
    OutputFile = open("/var/tmp/report.csv", 'a')
    OutputFile.write("HOSTNAME;IMPORTANT_INSTALLED;LOW_INSTALLED;MODERATE_INSTALLED;CRITICAL_INSTALLED;BUGFIX_INSTALLED;TOTAL_INSTALLED;SECURITY_INSTALLED;ENHANCEMENT_INSTALLED;SECURITY_PENDING;ENHANCEMENT_PENDING;BUGFIX_PENDING;APPLICABLE_RPM_PENDING;UPGRADABLE_RPM_PENDING;TOTAL_PENDING,CRITICAL_PENDING_30_DAYS\n")

    for Host in HostErratas:
        OutputFile.write(Host)

    OutputFile.close()

if __name__ == '__main__':
    cursor = connect(candlepin_db_user,candlepin_db_password,candlepin_db_name,posgtresql_hostname)
    cursor2 = connect(foreman_db_user,foreman_db_password,foreman_db_name,posgtresql_hostname)
    hosts_list = get_hosts(cursor)
    get_erratas(hosts_list,cursor,cursor2)
    write_errata_to_file()
    cursor.close()
    cursor2.close()
