#!/usr/bin/env python
# coding: utf-8

# # Case Study - 1; Danny's Diner

# ## Introduction

# Danny seriously loves Japanese food so in the beginning of 2021, he decides to embark upon a risky venture and opens up a cute little restaurant that sells his 3 favourite foods: sushi, curry and ramen.
# 
# Danny’s Diner is in need of your assistance to help the restaurant stay afloat - the restaurant has captured some very basic data from their few months of operation but have no idea how to use their data to help them run the business.

# ## Problem Statement

# Danny wants to use the data to answer a few simple questions about his customers, especially about their visiting patterns, how much money they’ve spent and also which menu items are their favourite. Having this deeper connection with his customers will help him deliver a better and more personalised experience for his loyal customers.
# 
# He plans on using these insights to help him decide whether he should expand the existing customer loyalty program - additionally he needs help to generate some basic datasets so his team can easily inspect the data without needing to use SQL.
# 
# Danny has provided you with a sample of his overall customer data due to privacy issues - but he hopes that these examples are enough for you to write fully functioning SQL queries to help him answer his questions!
# 
# 

# ###### Link to the dataset - https://docs.google.com/spreadsheets/d/1xXZPBEvnpkGhQVOGqlTqgGoHop5nulV0/edit#gid=1503140131

# ## Importing Libraries

# In[20]:


# import libraries
import pandas as pd
import sqlite3


# In[3]:


# install the ipython-sql libray
get_ipython().system('pip install ipython-sql')


# In[21]:


get_ipython().system('pip install duckdb==0.6.0')


# In[22]:


#importing duckdb library, allows us to run SQL queries on Pandas
import duckdb


# In[7]:


#importing different sheets of our dataset
ds01=pd.read_excel(r"D:\Practice Dashboards\8 Week SQL Challenge\Danny's_Diner.xlsx", sheet_name='sales')
ds02=pd.read_excel(r"D:\Practice Dashboards\8 Week SQL Challenge\Danny's_Diner.xlsx", sheet_name='menu')
ds03=pd.read_excel(r"D:\Practice Dashboards\8 Week SQL Challenge\Danny's_Diner.xlsx", sheet_name='members')


# In[8]:


cnn = sqlite3.connect('DannysDiner.db')


# In[12]:


#ds01 and ds02 have already been laoded
ds03.to_sql('members', cnn)
get_ipython().run_line_magic('load_ext', 'sql')


# In[13]:


get_ipython().run_line_magic('sql', 'sqlite:///DannysDiner.db')


# In[43]:


get_ipython().run_cell_magic('sql', '', 'SELECT * FROM sales')


# In[44]:


get_ipython().run_cell_magic('sql', '', 'SELECT * FROM menu')


# In[45]:


get_ipython().run_cell_magic('sql', '', 'SELECT * FROM members')


# ## Questions

# ##### Q1 - What is the total amount each customer spent at the restaurant?

# In[40]:


get_ipython().run_cell_magic('sql', '', '\nSELECT s.customer_id, sum(m.price) as total_amount_spent\nFROM sales as s\nLEFT JOIN menu as m ON s.product_id = m.product_id\nGROUP BY s.customer_id\nORDER BY s.customer_id')


# B is spending the highest amount, $89

# ###### Q2 - How many days has each customer visited the restaurant?

# In[41]:


get_ipython().run_cell_magic('sql', '', '\nSELECT s.customer_id, COUNT(DISTINCT s.order_date) as days_visited\nFROM sales as s\nGROUP BY s.customer_id\nORDER BY s.customer_id')


# A visited 4 times;
# B visited 6 times;
# C visited 2 times

# ###### Q3 - What was the first item from the menu purchased by each customer?

# In[48]:


get_ipython().run_cell_magic('sql', '', '\nSELECT sub.customer_id, m.product_name,sub.order_date\nFROM ( SELECT *,\nDENSE_RANK() over (PARTITION BY s.customer_id ORDER BY s.order_date asc) as rnk\nFROM sales s\n     ) sub\nLEFT JOIN menu ON sub.product_id = m.product_id\nWHERE rnk = 1')


# A's first order was Sushi and Curry;
# B's first order was Curry;
# C's first order was Ramen

# ###### Q4 - What is the most purchased item on the menu and how many times was it purchased by all customers?

# In[88]:


get_ipython().run_cell_magic('sql', '', '\nSELECT m.product_name, COUNT(s.product_id) as total_orders\nFROM sales s\nJOIN menu m ON s.product_id = m.product_id\nGROUP BY s.product_id,m.product_name\nORDER BY total_orders desc\nLIMIT 1')


# Most purchased item on the menu is Ramen and it is ordered 8 times

# ###### Q5 - Which item was the most popular for each customer?

# In[58]:


get_ipython().run_cell_magic('sql', '', '\nWITH fav_item AS (\nSELECT s.customer_id, m.product_name, COUNT(s.product_id) as order_count, \n  DENSE_RANK() OVER (PARTITION BY s.customer_id ORDER BY COUNT(s.product_id) DESC) as rnk\n  FROM sales s \n  LEFT JOIN menu m ON s.product_id = m.product_id\n  GROUP BY s.customer_id, m.product_name\n\n)\n\nSELECT customer_id, product_name, order_count \nFROM fav_item\nWHERE rnk = 1')


# A and C loves eating Ramen while B enjoys Curry

# ###### Q6 - Which item was purchased first by the customer after they became a member?

# In[61]:


get_ipython().run_cell_magic('sql', '', '\nWITH sales_post_membership AS (\nSELECT s.customer_id, s.order_date, m.join_date, s.product_id,\nDENSE_RANK() OVER (PARTITION BY s.customer_id ORDER BY s.order_date) as rnk\nFROM sales s \nLEFT JOIN members m ON s.customer_id = m.customer_id\nWHERE s.order_date >= m.join_date\n\n)\n\nSELECT spm.customer_id, mu.product_name\nFROM sales_post_membership spm\nLEFT JOIN menu mu ON spm.product_id = mu.product_id\nWHERE rnk = 1\nORDER BY spm.customer_id')


# When A became a member, his/her first order was Curry, whereas it was Sushi for B

# ###### Q7 - Which item was purchased just before the customer became a member? (Reverse of Q6)

# In[62]:


get_ipython().run_cell_magic('sql', '', '\nWITH sales_post_membership AS (\nSELECT s.customer_id, s.order_date, m.join_date, s.product_id,\nDENSE_RANK() OVER (PARTITION BY s.customer_id ORDER BY s.order_date) as rnk\nFROM sales s \nLEFT JOIN members m on s.customer_id = m.customer_id\nWHERE s.order_date <=  m.join_date\n\n)\n\nSELECT spm.customer_id, mu.product_name\nFROM sales_post_membership spm\nLEFT JOIN menu mu ON spm.product_id = mu.product_id\nWHERE rnk = 1\nORDER BY spm.customer_id')


# A's order before he/she became a customer is Sushi and Curry, while B's order is just Sushi. They must have been selling some great sushi!!

# ###### Q8 - What is the total items and amount spent for each member before they became a member?

# In[67]:


get_ipython().run_cell_magic('sql', '', '\nSELECT s.customer_id, COUNT(DISTINCT s.product_id) AS unique_items_ordered, SUM(mu.price) as total_sales\nFROM sales as s\nJOIN members as m\n ON s.customer_id = m.customer_id\nJOIN menu AS mu\n ON s.product_id = mu.product_id\nWHERE s.order_date < m.join_date\nGROUP BY s.customer_id')


# Before becoming a member, Customer A spent 25 dollars on 2 unique items, and Customer B spent 55 dollars on 2 unique items as well (B seems to be spending more as his/her number of unique items in orders before becoming a member could be greater than that of A)

# ###### Q9 - If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

# In[69]:


get_ipython().run_cell_magic('sql', '', '\nWITH order_points AS (\nSELECT *,\n  CASE\n  \tWHEN product_id = 1 then price * (2*10) \n  \tELSE price * 10\n  END as points\nFROM menu  \n)\n\nSELECT s.customer_id, SUM(o.points) as total_points \nFROM order_points o\nJOIN sales s \nON o.product_id = s.product_id\nGROUP BY s.customer_id')


# Total points for A, B, and C are 860, 1090, and 360, respectively

# ###### Q10 - In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?

# In[113]:


get_ipython().run_cell_magic('sql', '', "\nWITH cte AS (\n  \tSELECT *, DATE(m.join_date, '+6 days') AS valid_date, DATE('2021-01-31') AS last_date\n\tFROM members m)\n\nSELECT s.customer_id, \nSUM(CASE\n        WHEN mu.product_id = 1 then mu.price * (2*10)\n        WHEN s.order_date between d.join_date and d.valid_date then \tmu.price * (2*10)\n        ELSE mu.price * 10\n    END) as points\nFROM cte d\nJOIN sales s \nON s.customer_id = d.customer_id\nJOIN menu mu \nON s.product_id = mu.product_id\nWHERE s.order_date < d.last_date\nGROUP BY s.customer_id\n\n")


# A has 1370 points while B has 970 points

# ### Some useful insights for Danny

# 1. Customer B is the most frequent visitor with 7 visits in Jan 2021.
# 2. Danny’s Diner’s most popular item is ramen, followed by curry and sushi.
# 3. Customer A and C loves ramen whereas Customer B seems to enjoy sushi, curry and ramen equally.
# 4. Customer A is the 1st member of Danny’s Diner and his first order is curry.
# 5. The last item ordered by Customers A and B before they became members are sushi and curry.
# 7. Before they became members, Customers A and B spent 25 dollars  and 55 dollars, respectively.
# 8. Throughout Jan 2021, their points for Customer A: 860, Customer B: 1090 and Customer C: 360.

# # Thank you!
