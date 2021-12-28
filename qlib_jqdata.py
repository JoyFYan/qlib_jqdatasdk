import jqdatasdk
username, password = '15680893289', '455000YzYabC'
jqdatasdk.auth(username, password)
jqdatasdk.get_price("000001.XSHE", start_date="2017-01-01", end_date="2017-12-31")