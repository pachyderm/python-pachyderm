# Jupyter Notebook using Versioned Pachyderm Data

In this example, we'll use a combination of Jupyter notebooks, Pandas, and Pachyderm to analyze Citi Bike sales data. Make sure you have these dependencies installed:

* Pandas
* Matplotlib
* Jupyter

Then point your Jupyter notebook to `investigate-unexpected-sales.ipynb` in this directory.

Our challenge is to figure out why there was a sharp drop in sales on 7/30/16 and 7/31/16:

![alt tag](sales.png)

This analysis will confirm that, on the days in questions, there was a 70%+ chance of rain, and this is likely the reason for poor bike sharing sales. Mystery solved! Here is the graph that we created:

![alt tag](final_graph.png)
