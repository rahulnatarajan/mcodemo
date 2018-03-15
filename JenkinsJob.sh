curl -v -X POST http://URL/jenkins/job/MCODemo-Presto-Creation/buildWithParameters \
--data token=MCODemo-Presto-Create \
--user rahulna:Welcome123$ \
--data-urlencode json='{"parameter": [{"name”:"timekey", "value”:"2017-10-31"}, {“name”:"tablename", "value":"mv_mco_account"}]}'
