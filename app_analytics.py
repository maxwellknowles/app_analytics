#requirements
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore
from datetime import datetime, date
from st_aggrid import AgGrid
import streamlit as st
from geopy.geocoders import Nominatim
import statistics
import numpy as np
import altair as alt
import json
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from kneed import KneeLocator
import matplotlib.pyplot as plt
import plotly.express as px
import ast

#page setup
st.set_page_config(page_title="Chptr Analytics", page_icon=":rocket:", layout="wide",initial_sidebar_state="expanded")

#JSON_DATA = {"key":st.secrets['google_key_file']}
secret = str(st.secrets['google_key_file'])
JSON_DATA = ast.literal_eval(secret)

#functions
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def connect_to_firestore():
    #connecting to firebase
    cred = credentials.Certificate(JSON_DATA)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://console.firebase.google.com/u/2/project/chptr-b101d/firestore/data'
    })
connect_to_firestore()
    
db = firestore.client()

#users collection to dataframe
@st.cache(suppress_st_warning=True, allow_output_mutation=True, ttl=43200)
def get_users():
    users = list(db.collection(u'users').stream())
    users_dict = list(map(lambda x: x.to_dict(), users))
    users = pd.DataFrame(users_dict)
    l=[]
    for i in range(len(users)):
        selectedChptrID = users['selectedChptrID'][i]
        user_id = users['id'][i]
        try:
            name = users['firstName'][i] + " " + users['lastName'][i]
        except TypeError:
            name = None
        pendingChptrRequests = users["pendingChptrRequests"][i]
        count_pendingChptrRequests = len(pendingChptrRequests)
        email_allowed = users["allowsReceiveEmail"][i]
        state = users["state"][i]
        chptrIds = users["chptrIds"][i]
        number_chptrs = len(users["chptrIds"][i])
        tup = (selectedChptrID, 
            user_id,
            name,
            pendingChptrRequests,
            count_pendingChptrRequests,
            email_allowed,
            state,
            chptrIds,
            number_chptrs
            )
        l.append(tup)
    users_consolidated = pd.DataFrame(l,columns=["Selected Chptr ID",
                                                "User ID",
                                                "User Name",
                                                "Pending Chptr Requests",
                                                "Count Pending Chptr Requests",
                                                "Email Allowed",
                                                "State",
                                                "Chptr IDs",
                                                "Number of Chptrs"
                                                ])
    return users_consolidated

#chptrs collection to dataframe
@st.cache(suppress_st_warning=True, allow_output_mutation=True, ttl=43200)
def get_chptrs():
    chptrs = list(db.collection(u'chptrs').stream())
    chptrs_dict = list(map(lambda x: x.to_dict(), chptrs))
    chptrs = pd.DataFrame(chptrs_dict)
    l=[]
    for i in range(len(chptrs)):
        chptrId = chptrs['id'][i]
        owner = chptrs['owner'][i]
        chptr_name = chptrs['firstName'][i] + " " + chptrs['lastName'][i]
        birthday = chptrs['birthday'][i]
        passing_date = chptrs['passingDate'][i]
        location = chptrs['location'][i]
        if len(location)>0:
            try:
                geolocator = Nominatim(user_agent="Chptr")
                city = geolocator.geocode(location)
                lat = city.latitude
                lon = city.longitude
            except:
                lat = None
                lon = None
        else:
            lat = None
            lon = None
        description = chptrs['description'][i]
        length_description = len(chptrs['description'][i])
        profile_image = chptrs['profileImageUrl'][i]
        if profile_image != None:
            profile_image_boolean = True
        else:
            profile_image_boolean = False
        pending_requests = chptrs['numberOfPendingRequests'][i]
        contributors = chptrs['contributors'][i]
        count_contributors = len(chptrs['contributors'][i])
        tup = (chptrId, 
            owner,
            chptr_name,
            birthday,
            passing_date,
            location,
            lat,
            lon,
            description,
            length_description,
            profile_image,
            profile_image_boolean,
            pending_requests,
            contributors,
            count_contributors)
        l.append(tup)
    chptrs_consolidated = pd.DataFrame(l,columns=["Chptr ID",
                                                "Chptr Owner",
                                                "Name",
                                                "Birthday",
                                                "Passing Date",
                                                "Location",
                                                "Lat",
                                                "Lon",
                                                "Description",
                                                "Length of Description",
                                                "Profile Image URL",
                                                "Profile Image Present",
                                                "Pending Requests",
                                                "Contributors",
                                                "Number of Contributors"])
    return chptrs_consolidated

#contributions collection to dataframe
@st.cache(suppress_st_warning=True, allow_output_mutation=True, ttl=43200)
def get_contributions():
    contributions = list(db.collection(u'contributions').stream())
    contributions_dict = list(map(lambda x: x.to_dict(), contributions))
    contributions = pd.DataFrame(contributions_dict)
    l=[]
    for i in range(len(contributions)):
        contribution_id = contributions['id'][i]
        chptrId = contributions['chptrId'][i]
        chptr_name = contributions['chptrName'][i]
        date = contributions['creationDate'][i]
        date_split = date.split("T")[0]
        month = datetime.strptime(date_split, '%Y-%m-%d').month
        description = contributions['description'][i]
        description_length = len(contributions['description'][i])
        owner = contributions['owner'][i]['name']
        contributors = contributions['contributors'][i]
        count_contributors = len(contributions['contributors'][i])
        categories = contributions['categories'][i]
        try:
            count_categories = len(contributions['categories'][i])
        except TypeError:
            count_categories = 0
        comments = contributions['amountOfComments'][i]
        likes = contributions['userLikesIds'][i]
        count_likes = len(contributions['userLikesIds'][i])
        tup = (contribution_id,
            chptrId, 
            chptr_name, 
            date, 
            month,
            description, 
            description_length,
            owner,
            contributors, 
            count_contributors,
            categories,
            count_categories,
            comments, 
            likes,
            count_likes)
        l.append(tup)
        contributions_consolidated = pd.DataFrame(l,columns=["Contribution ID",
                                                            "Chptr ID", 
                                                            "Chptr Name", 
                                                            "Date", 
                                                            "Month",
                                                            "Description", 
                                                            "Length of Description",
                                                            "Owner", 
                                                            "Contributors",
                                                            "Count Contributors",
                                                            "Categories",
                                                            "Count Categories",
                                                            "Comments",
                                                            "Likes",
                                                            "Count Likes"])
    return contributions_consolidated

#get data
users = get_users()
chptrs = get_chptrs()
contributions = get_contributions()

user_contributions = []
for i in range(len(chptrs)):
    try:
        user_contributions += chptrs["Contributors"][i]
    except TypeError:
        pass

values, counts = np.unique(user_contributions, return_counts=True)

df = {"User ID":values, "Count of Contributions":counts}
user_contributions = pd.DataFrame(df)
users_with_contribution_count = pd.merge(users, user_contributions, how='inner', on = 'User ID')
users_with_contribution_count = users_with_contribution_count.reset_index()

categories = []
for i in range(len(contributions)):
    try:
        categories += contributions["Categories"][i]
    except TypeError:
        pass

kindergarten = categories.count('Kindergarten')
lost_weekend = categories.count('Lost Weekend')
lower_school = categories.count('Lower School')
roaming = categories.count('Roaming')
grade_school = categories.count('Grade School')
sabbatical = categories.count('Sabbatical')
middle_school = categories.count('Middle School')
military = categories.count('Military')
work = categories.count('Work')
family = categories.count('Family')
retirement = categories.count('Retirement')
values = [kindergarten, 
    lost_weekend, 
    lower_school, 
    roaming, 
    grade_school, 
    sabbatical, 
    middle_school, 
    military, 
    work, 
    family, 
    retirement]

category_titles = ["Kindergarten",
    "Lost Weekend",
    "Lower School",
    "Roaming",
    "Grade School",
    "Sabbatical",
    "Middle School",
    "Military",
    "Work",
    "Family",
    "Retirement"]

df = {"Category":category_titles, "Count of Contributions":values}
category_performance = pd.DataFrame(df)
category_performance = category_performance.set_index('Category')

chptrs_owners = pd.merge(chptrs, users, how='inner', left_on = 'Chptr Owner', right_on="User ID")

chptrs_ordered = contributions.sort_values("Date", ascending=True).drop_duplicates(["Chptr ID"])
contributions_grouped = contributions.groupby("Chptr ID").agg({"Comments": 'sum', "Count Likes": 'sum'})
chptrs_ordered = pd.merge(contributions_grouped, chptrs_ordered, how='inner', on = 'Chptr ID')
chptrs_ordered = chptrs_ordered[(chptrs_ordered["Owner"]!="Ana Beta 1 Test")]
chptrs_ordered = chptrs_ordered[(chptrs_ordered["Owner"]!="Ani Beta Test")]
chptrs_ordered = chptrs_ordered[(chptrs_ordered["Owner"]!="Ani test beta Test")]
chptrs_ordered = chptrs_ordered[(chptrs_ordered["Owner"]!="Anita Beta Test")]
chptrs_ordered = chptrs_ordered[(chptrs_ordered["Owner"]!="Test Test")]
chptrs_ordered = chptrs_ordered[(chptrs_ordered["Owner"]!="Beta Test")]
chptrs_ordered = pd.merge(chptrs, chptrs_ordered, how='outer', on = 'Chptr ID')

contributions_sorted_date = contributions.sort_values("Chptr ID", ascending=True)
contributions_sorted_date = contributions_sorted_date.sort_values("Date", ascending=True)
contributions_sorted = contributions_sorted_date.reset_index(drop=True)
contributions_sorted_no_dupes = contributions_sorted.drop_duplicates(["Chptr ID"])
contributions_sorted_no_dupes = contributions_sorted_no_dupes.reset_index(drop=True)
contributions_sorted_list=[]
for i in range(len(contributions_sorted_no_dupes)):
    chptr_id = contributions_sorted_no_dupes['Chptr ID'][i]
    action_type = 'Contribution'
    for j in range(len(contributions_sorted_no_dupes['Contributors'][i])):
        contributor = contributions_sorted_no_dupes['Contributors'][i][j]
        tup=(chptr_id,action_type,contributor)
        contributions_sorted_list.append(tup)
contributions_sorted = pd.DataFrame(contributions_sorted_list, columns=['Chptr ID', 'Action Type', 'User ID'])
contributions_sorted = contributions_sorted.reset_index(drop=True)
contributions_sorted = pd.merge(contributions_sorted, contributions_sorted_date, how='inner', on = 'Chptr ID')
contributions_sorted = contributions_sorted.drop_duplicates(["Date"])
contributions_sorted = contributions_sorted.reset_index(drop=True)

@st.cache(suppress_st_warning=True, allow_output_mutation=True, ttl=43200)
def get_comments():
    l=[]
    for i in range(len(contributions)):
        contribution_id=contributions['Contribution ID'][i]
        doc_ref = db.collection('contributions').document(contribution_id).collection('comments')
        for doc in doc_ref.stream():
            item1 = doc_ref.document(doc.id)
            item2 = item1.get().to_dict()
            tup=(item2)
            l.append(tup)
    comments = pd.DataFrame(l)
    return comments

comments = get_comments()
comments = comments[comments['owner'].notna()]
comments = comments.reset_index(drop=True)
k=[]
for i in range(len(comments)):
    comment_id = comments['id'][i]
    user = comments['owner'][i]['id']
    date = comments['timeOfCreation'][i]
    action_type = 'Comment'
    tup = (date, comment_id, user, action_type)
    k.append(tup)
comments_consolidated=pd.DataFrame(k, columns=['Date', 'Comment ID', 'User ID', 'Action Type'])
comments_consolidated=comments_consolidated.sort_values("Date", ascending=True)
comments_consolidated=comments_consolidated.reset_index(drop=True)

#download data
@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

#START OF STREAMLIT PAGE
st.header("Chptr Analytics")
st.write("**Key Assumptions and Notes**")
#st.write("• Chptr has yet to make a push on marketing")
st.write("• Chptr has yet to monetize Chptr creation, but that is what should present a 'transaction' event")
st.write("• Contributions to Chptrs represent engagement, but are not considered monetizable actions, now or in the future")
st.subheader("High Level Stats")
col1, col2, col3 = st.columns(3)
with col1:
    st.write("**USERS**")
    st.metric("Number of Users",len(users))
    st.metric("Users with Contributions",len(users_with_contribution_count))
    st.metric("Contributions per User", round(len(contributions)/len(users),2))
with col2:
    st.write("**CHPTRS**")
    st.metric("Number of Chptrs",len(chptrs))
    st.metric("Number of Cities with Chptrs",len(pd.unique(chptrs['Location'])))
    owners_agg = chptrs_ordered.groupby("Owner").agg({"Owner": 'count'})
    st.metric("Number of Chptrs per Owner",round(statistics.mean(owners_agg['Owner']),2))
with col3:
    st.write("**CONTRIBUTIONS**")
    st.metric("Number of Contributions",len(contributions))
    st.metric("Contributions per Chptr",round(len(contributions)/len(chptrs),2))
    st.metric("Contributors per Chptr",round(sum(chptrs["Number of Contributors"])/len(chptrs),2))
    #st.metric("Categories per Contribution",round(statistics.mean(contributions["Count Categories"]),2))
    st.metric("Comments per Contribution",round(statistics.mean(contributions["Comments"]),2))

#with col4:
    #st.write("**RATINGS**")
    #st.metric("Five-Star Reviews", "100%", "153 reviews")

contributions_categories = contributions[['Month','Count Categories']]
contributions_categories = contributions_categories.set_index("Month")
#st.bar_chart(contributions_categories)

#graph of contributors
contributions_contributors = contributions[['Month','Count Contributors']]
contributions_contributors = contributions_contributors.set_index("Month")
#st.bar_chart(contributions_contributors)

contributions_max_contributors = contributions.sort_values('Count Contributors', ascending=False).drop_duplicates(['Chptr ID'])
contributions_max_contributors = contributions_max_contributors[["Month","Count Contributors"]]
contributions_max_contributors = contributions_max_contributors.set_index("Month")

tab1, tab2, tab3, tab4 = st.tabs(["Explore Chptrs", "Explore Contributions", "Explore Users", "Download Data"])

with tab1:
    #chptrs over time
    st.subheader("Chptr Publications by Month")
    chptrs_ordered_publications = chptrs_ordered.groupby("Month").agg({"Month": 'count'})
    chptrs_ordered_publications = chptrs_ordered_publications.rename_axis('Month Number')
    chptrs_ordered_publications["Chptrs"] = chptrs_ordered_publications["Month"]
    chptrs_ordered_publications = chptrs_ordered_publications.drop("Month", axis=1)
    st.bar_chart(chptrs_ordered_publications)

    st.subheader("Chptrs by Completion Characteristics")

    #chptrs with image
    chptrs_with_image = chptrs[(chptrs['Profile Image Present']==True)]
    count_chptrs_with_image = len(chptrs_with_image)
    count_chptrs_with_image_contributors = (statistics.mean(chptrs_with_image["Number of Contributors"]))

    #chptrs with dates
    chptrs_with_dates = chptrs[(chptrs['Birthday'].notnull())]
    count_chptrs_with_dates = len(chptrs_with_dates)
    count_chptrs_with_dates_contributors = (statistics.mean(chptrs_with_dates["Number of Contributors"]))

    #chptrs with locations
    chptrs_with_locations = chptrs[(chptrs['Location']!="")]
    count_chptrs_with_locations = len(chptrs_with_locations)
    count_chptrs_with_locations_contributors = (statistics.mean(chptrs_with_locations["Number of Contributors"]))

    #chptrs with locations
    chptrs_with_locations_and_dates = chptrs_with_locations[(chptrs_with_locations['Birthday'].notnull())]
    count_chptrs_with_locations_and_dates = len(chptrs_with_locations_and_dates)
    count_chptrs_with_locations_and_dates_contributors = (statistics.mean(chptrs_with_locations_and_dates["Number of Contributors"]))

    #chptrs with description
    chptrs_with_descriptions = chptrs[(chptrs['Length of Description']>0)]
    count_chptrs_with_descriptions = len(chptrs_with_descriptions)
    count_chptrs_with_descriptions_contributors = (statistics.mean(chptrs_with_descriptions["Number of Contributors"]))
    
    chptr_types = ["Chptrs with Dates", "Chptrs with Locations", "Chptrs with Descriptions", "Chptrs with Profile Image", "All"]
    chptr_counts = [count_chptrs_with_dates, count_chptrs_with_locations, count_chptrs_with_descriptions, count_chptrs_with_image, len(chptrs)]
    chptr_contributors = [count_chptrs_with_dates_contributors, count_chptrs_with_locations_contributors, count_chptrs_with_descriptions_contributors, count_chptrs_with_image_contributors, statistics.mean(chptrs["Number of Contributors"])]
    d = {"Chptrs Type":chptr_types, "Type Count":chptr_counts, "Type Contributors":chptr_contributors}
    chptrs_analysis = pd.DataFrame(d)
    st.dataframe(chptrs_analysis)

    c = alt.Chart(chptrs_analysis).mark_circle().encode(
    x='Type Count', y='Type Contributors', size='Type Count', color='Chptrs Type', tooltip=['Chptrs Type', 'Type Count', 'Type Contributors'])

    st.altair_chart(c, use_container_width=True)

    #getting birthday and passing date data
    chptrs_passing_data = chptrs_ordered[["Chptr ID", "Birthday","Passing Date", "Date"]]
    chptrs_passing_data = chptrs_passing_data[chptrs_passing_data['Passing Date'].notna()]
    chptrs_passing_data = chptrs_passing_data[chptrs_passing_data['Date'].notna()]
    chptrs_passing_data = chptrs_passing_data.reset_index(drop=True)
    passing_delta = []
    for i in range(len(chptrs_passing_data)):
        chptr_id=chptrs_passing_data["Chptr ID"][i]
        birth_date=str(chptrs_passing_data["Birthday"][i]).split("T")[0]
        birth_date=datetime.strptime(birth_date, '%Y-%m-%d').year
        passing_date=str(chptrs_passing_data["Passing Date"][i]).split("T")[0]
        passing_date=datetime.strptime(passing_date, '%Y-%m-%d').year
        age=passing_date-birth_date
        chptr_date=str(chptrs_passing_data["Date"][i]).split("T")[0]
        chptr_date = datetime.strptime(chptr_date, '%Y-%m-%d').year
        delta=chptr_date-passing_date
        tup=(chptr_id, age, delta)
        passing_delta.append(tup)
    chptrs_passing=pd.DataFrame(passing_delta, columns=['Chptr ID', "Age", "Years Passed Before Chptr Creation"])
    chptrs_passing=chptrs_passing[(chptrs_passing["Years Passed Before Chptr Creation"]>=0)]
    chptrs_passing=chptrs_passing[(chptrs_passing["Age"]<111)]
    
    chptrs_agg = chptrs_passing.groupby("Years Passed Before Chptr Creation").agg({"Years Passed Before Chptr Creation": 'count'})
    chptrs_agg["Count of Chptrs"] = chptrs_agg["Years Passed Before Chptr Creation"]
    chptrs_agg = chptrs_agg.drop("Years Passed Before Chptr Creation", axis=1)
    chptrs_agg = chptrs_agg.rename_axis("Years Passed Before Chptr Creation")
    chptrs_agg = chptrs_agg.reset_index()
    c = alt.Chart(chptrs_agg).mark_circle().encode(
        x="Years Passed Before Chptr Creation", y="Count of Chptrs", size="Count of Chptrs", color="Count of Chptrs", tooltip=["Years Passed Before Chptr Creation", "Count of Chptrs"])

    st.subheader("Chptr Count and Years Relative to Passing")
    st.metric("Average Years After Passing", round(statistics.mean(chptrs_passing["Years Passed Before Chptr Creation"]),2))
    st.altair_chart(c, use_container_width=True)

    chptrs_age = chptrs_passing.groupby("Age").agg({"Chptr ID": 'count'})
    chptrs_age["Count of Chptrs"] = chptrs_age["Chptr ID"]
    chptrs_age = chptrs_age.drop("Chptr ID", axis=1)
    chptrs_age = chptrs_age.rename_axis("Age")
    chptrs_age = chptrs_age.reset_index()

    c = alt.Chart(chptrs_age).mark_circle().encode(
        x="Age", y="Count of Chptrs", size="Count of Chptrs", color="Count of Chptrs", tooltip=["Age", "Count of Chptrs"])

    st.subheader("Chptr Count and Age of Tributee")
    st.metric("Average Age", round(statistics.mean(chptrs_passing["Age"]),2))
    st.altair_chart(c, use_container_width=True)

    #graph of locations
    st.subheader("Chptrs by City")
    chptrs_lat_lon = chptrs[['Lat','Lon']]
    chptrs_lat_lon=chptrs_lat_lon[chptrs_lat_lon.Lat.notnull()]
    chptrs_lat_lon['lat']=chptrs_lat_lon['Lat']
    chptrs_lat_lon['lon']=chptrs_lat_lon['Lon']
    st.map(chptrs_lat_lon)

with tab2:
    #contributions per chptr over time
    st.subheader("Avg Count of Contributors per Chptr by Month")
    chptrs_ordered_contributors = chptrs_ordered[["Month","Count Contributors"]]
    chptrs_ordered_contributors = chptrs_ordered_contributors.groupby("Month").agg({"Count Contributors": 'mean'})
    st.bar_chart(chptrs_ordered_contributors)

    st.subheader("Engagement Span of Chptr Contribution")
    #sorting contributions by Chptr ID and date of contributions, getting the latest contribution for each
    contributions_organized_date_descending = contributions.sort_values("Date", ascending=False)
    #contributions_organized_date_ascending = contributions.sort_values("Date", ascending=True)
    #contributions_organized = contributions_organized_date_ascending.reset_index(drop=True)
    contributions_organized = contributions.sort_values(by=["Chptr ID", "Date"], ascending=[False,True])
    contributions_organized = contributions_organized.reset_index(drop=True)
    contributions_organized_narrowed = contributions_organized_date_descending.drop_duplicates('Chptr ID')
    contributions_organized_narrowed = contributions_organized_narrowed.reset_index(drop=True)

    #creating dataframe with every contribution and its days since the first contribution (less than 1 rounds to 1)
    l=[]
    count = 1
    gap = 0
    time_total = 0
    for i in range(len(contributions_organized)):
        chptr_id = contributions_organized["Chptr ID"][i]
        chptr_name = contributions_organized["Chptr Name"][i]
        chptr_date = contributions_organized["Date"][i]
        if i>0 and chptr_id == contributions_organized["Chptr ID"][i-1]:
            count += 1
            day_now = contributions_organized["Date"][i]
            day_now_1 = day_now.split("T")[0]
            day_now_2 = day_now.split("T")[1]
            day_now_2 = day_now_2.split("Z")[0]
            day_now = day_now_1+" "+day_now_2
            #day_now = datetime.strptime(day_now, '%Y-%m-%d')
            day_now = datetime.strptime(day_now, '%Y-%m-%d %H:%M:%S.%f')
            day_last = contributions_organized["Date"][i-1]
            day_last_1 = day_last.split("T")[0]
            day_last_2 = day_last.split("T")[1]
            day_last_2 = day_last_2.split("Z")[0]
            day_last = day_last_1+" "+day_last_2
            #day_last = datetime.strptime(day_last, '%Y-%m-%d')
            day_last = datetime.strptime(day_last, '%Y-%m-%d %H:%M:%S.%f')
            day_gap = day_now - day_last
            day_gap_seconds = day_gap.seconds
            time_total = time_total + day_gap_seconds
            time_total_hours = time_total/(3600*24)
            #hour_now = (hour_now.seconds)/60
            gap = day_gap_seconds
            gap_hours = gap/(3600*24)
            #hour = (hour.seconds)/60
        else:
            count = 1
            gap_hours = 0
            time_total = 0
            time_total_hours = 0
        #contribution_date = contributions_organized["Date"][i]
        #contribution_date = contribution_date.split("T")[0]
        #contribution_date = datetime.strptime(contribution_date, '%Y-%m-%d')
        tup = (chptr_id, chptr_name, chptr_date, gap_hours, time_total_hours, count)
        l.append(tup)
    chptrs_cohort = pd.DataFrame(l, columns=["Chptr ID", "Chptr Name", "Date", "Days Between Contributions", "Days Between First and Last", "Count of Contributions"])

    chptrs_cohort_last = chptrs_cohort.sort_values("Days Between First and Last", ascending=False)
    chptrs_cohort_last = chptrs_cohort_last.drop_duplicates("Chptr Name")
    chptrs_cohort_last = chptrs_cohort_last.reset_index()

    kmeans_chptrs = chptrs_cohort_last[["Days Between First and Last", 'Count of Contributions']]
    kmeans_chptrs_scaled = StandardScaler().fit_transform(kmeans_chptrs)
    kmeans_chptrs_scaled = pd.DataFrame(kmeans_chptrs_scaled, columns = kmeans_chptrs.columns)

    #kmeans clustering using latest contribution and count of total contributions
    sse = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k, init='k-means++', max_iter=300, n_init=10, random_state=42)
        kmeans.fit(kmeans_chptrs_scaled)
        sse.append(kmeans.inertia_)
    kl = KneeLocator(range(1, 11), sse, curve="convex", direction="decreasing")
    n = kl.elbow
    kmeans = KMeans(n_clusters=n, init='k-means++', max_iter=300, n_init=10, random_state=42)
    pred_y = kmeans.fit_predict(kmeans_chptrs_scaled)
    kmeans_chptrs['Classification'] = pd.Series(pred_y, index=kmeans_chptrs.index)
    chptrs_cohort_last['Classification'] = pd.Series(pred_y, index=kmeans_chptrs.index)
    chptrs_cohort_last = chptrs_cohort_last.drop("Days Between Contributions",axis=1)
    chptrs_cohort_last = chptrs_cohort_last.reset_index(drop=True)

    fig = px.scatter(
        x=kmeans_chptrs["Days Between First and Last"],
        y=kmeans_chptrs['Count of Contributions'],
        color=kmeans_chptrs['Classification']
    )
    fig.update_layout(
        xaxis_title="Days Between First and Last",
        yaxis_title='Count of Contributions',
    )

    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)

    scatter = ax.scatter(
        x=kmeans_chptrs["Days Between First and Last"],
        y=kmeans_chptrs['Count of Contributions'],
        c=kmeans_chptrs['Classification'],
        label=kmeans_chptrs['Classification']
    )

    legend = ax.legend(*scatter.legend_elements(),
                loc="lower right", title="Chptr Cluster")

    ax.set_xlabel("Days Between First and Last")
    ax.set_ylabel("Count of Contributions")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Average Days Between Last and First Contribution", str(round(sum(chptrs_cohort_last["Days Between First and Last"])/len(chptrs_cohort_last),2)))
        st.dataframe(chptrs_cohort_last)

        chptrs_cohort_csv = convert_df(chptrs_cohort)

        st.download_button(
            label="Download Chptr contribution cohorts as CSV",
            data=chptrs_cohort_csv,
            file_name='chptr_contribution_cohorts.csv',
            mime='text/csv',
            )
    with col2:
        st.write(fig)

    st.subheader("Days Since Last Contribution by Chptr")
    #today
    today = datetime.today()
    
    l=[]
    for i in range(len(contributions_organized_narrowed)):
        chptr_id = contributions_organized_narrowed["Chptr ID"][i]
        chptr_name = contributions_organized_narrowed["Chptr Name"][i]
        latest_contribution = contributions_organized_narrowed["Date"][i]
        latest_contribution = latest_contribution.split("T")[0]
        latest_contribution = datetime.strptime(latest_contribution, '%Y-%m-%d')
        days_since = today-latest_contribution
        days_since = days_since.days
        tup = (chptr_id, chptr_name, latest_contribution, days_since)
        l.append(tup)
    days_since_latest_contribution = pd.DataFrame(l, columns=["Chptr ID", "Chptr Name", "Latest Contribution", "Days Since"]) 
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("3 or more days since a contribution", len(days_since_latest_contribution[(days_since_latest_contribution["Days Since"]>3)]))
    with col2:
        st.metric("7 or more days since a contribution", len(days_since_latest_contribution[(days_since_latest_contribution["Days Since"]>7)]))
    with col3:
        st.metric("30 or more days since a contribution", len(days_since_latest_contribution[(days_since_latest_contribution["Days Since"]>30)]))
    with col4:
        st.metric("90 or more days since a contribution", len(days_since_latest_contribution[(days_since_latest_contribution["Days Since"]>90)]))

    st.dataframe(days_since_latest_contribution)

    #comments on contributions over time
    st.subheader("Avg Count of Comments per Contribution by Month")
    chptrs_ordered_comments = chptrs_ordered[["Month","Comments_y"]]
    chptrs_ordered_comments = chptrs_ordered_comments.groupby("Month").agg({"Comments_y": 'mean'})
    st.bar_chart(chptrs_ordered_comments)

    #likes on contributions over time
    st.subheader("Avg Count of Likes per Contribution by Month")
    chptrs_ordered_comments = chptrs_ordered[["Month","Count Likes_y"]]
    chptrs_ordered_comments = chptrs_ordered_comments.groupby("Month").agg({"Count Likes_y": 'mean'})
    st.bar_chart(chptrs_ordered_comments)

    #length of description for contribution over time
    st.subheader("Avg Length of Description per Contribution by Month (Characters)")
    chptrs_ordered_description = chptrs_ordered[["Month","Length of Description_x"]]
    chptrs_ordered_description = chptrs_ordered_description.groupby("Month").agg({"Length of Description_x": 'mean'})
    st.bar_chart(chptrs_ordered_description)

l=[]

with tab3:
    st.subheader("Users with Pending Chptr Requests")
    users_pending_invites = users[(users["Count Pending Chptr Requests"]>0)]
    users_pending_invites = users_pending_invites.reset_index(drop=True)
    for i in range(len(users_pending_invites)):
        user_id = users_pending_invites["User ID"][i]
        user_name = users_pending_invites["User Name"][i]
        for k in users_pending_invites["Pending Chptr Requests"][i]:
            pending_chptr = k
            pending_chptr_invite = "https://chptrprod.page.link?amv=0&apn=com.chptr.chptr&ibi=com.chptr.chptr&imv=0&isi=1620239435&link=https%3A%2F%2Fchptrprod.page.link%2Fview%3FchptrID%3D"+pending_chptr
            tup = (user_id, user_name, pending_chptr, pending_chptr_invite)
            l.append(tup)
    users_pending = pd.DataFrame(l, columns=["User ID", "User Name", "Pending Chptr", "Invitation Link"])
    st.write("Count of pending requests: ", len(users_pending))
    users_pending = users_pending.sort_values("User ID")
    users_pending = users_pending.reset_index(drop=True)
    users_pending = pd.merge(users_pending, chptrs_ordered, how='left', left_on="Pending Chptr", right_on="Chptr ID")
    users_pending = users_pending[["User ID", "User Name", "Pending Chptr", "Invitation Link", "Date"]]
    users_pending["Chptr Date"] = users_pending["Date"]
    users_pending = users_pending.drop("Date", axis=1)
    st.dataframe(users_pending)

    st.subheader("Users without a chptr or contribution")
    owners_list = []
    contributors_list = []
    for i in range(len(chptrs)):
        owner = chptrs["Chptr Owner"][i]
        tup = (owner)
        owners_list.append(owner)
        contributors = chptrs["Contributors"][i]
        contributors_list += contributors
        #contributors_list.append(contributor)
    final_list = owners_list + contributors_list
    final_set = set(final_list)

    users_list = []
    for i in range(len(users)):
        user = users["User ID"][i]
        tup = (user)
        users_list.append(user)
    
    users_set = set(users_list)
    unactivated_list = users_set - final_set 

    l=[]
    for i in range(len(users)):
        if users["User ID"][i] in unactivated_list:
            user_id = users["User ID"][i]
            name = users["User Name"][i]
            tup = (user_id, name)
            l.append(tup)
    unactivated_df = pd.DataFrame(l, columns=["User ID", "User Name"])
    st.write("Users without a chptr or contribution: ", len(unactivated_df))
    st.dataframe(unactivated_df)

    #download unactivated data
    st.write("**Unactivated data**")

    unactivated_df_csv = convert_df(unactivated_df)

    st.download_button(
        label="Download unactivated user data as CSV",
        data=unactivated_df_csv,
        file_name='unactivated_users.csv',
        mime='text/csv',
        )


    users_chptrs = chptrs.groupby("Chptr Owner").agg({"Chptr ID":"count"})
    users_chptrs["Chptrs Created"] = users_chptrs["Chptr ID"]
    users_chptrs = users_chptrs.drop("Chptr ID", axis=1)
    users_chptrs = users_chptrs.rename_axis("User ID")
    users_chptrs = users_chptrs.reset_index()
    
    l=[]
    for i in range(len(contributors_list)):
        user_id = contributors_list[i]
        count = contributors_list.count(user_id)
        tup = (user_id, count)
        l.append(tup)
    users_contributors = pd.DataFrame(l, columns=["User ID", "Chptrs as Contributor"])
    users_contributors = users_contributors.drop_duplicates("User ID")
    active_users = pd.merge(users_chptrs, users_contributors, how='outer', on="User ID")
    for i in range(len(active_users)):
        if pd.isna(active_users["Chptrs Created"][i]):
            active_users["Chptrs Created"][i]=0

    #kmeans for user activity
    kmeans_active_users = active_users[["Chptrs Created", 'Chptrs as Contributor']]
    kmeans_active_users_scaled = StandardScaler().fit_transform(kmeans_active_users)
    kmeans_active_users_scaled = pd.DataFrame(kmeans_active_users_scaled, columns = kmeans_active_users.columns)

    #kmeans clustering using latest contribution and count of total contributions
    sse = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k, init='k-means++', max_iter=300, n_init=10, random_state=42)
        kmeans.fit(kmeans_active_users_scaled)
        sse.append(kmeans.inertia_)
    kl = KneeLocator(range(1, 11), sse, curve="convex", direction="decreasing")
    n = kl.elbow
    kmeans = KMeans(n_clusters=n, init='k-means++', max_iter=300, n_init=10, random_state=42)
    pred_y = kmeans.fit_predict(kmeans_active_users_scaled)
    kmeans_active_users['Classification'] = pd.Series(pred_y, index=kmeans_active_users.index)
    active_users['Classification'] = pd.Series(pred_y, index=kmeans_active_users.index)
    #chptrs_cohort_last = chptrs_cohort_last.drop("Days Between Contributions",axis=1)
    #chptrs_cohort_last = chptrs_cohort_last.reset_index(drop=True)

    fig = px.scatter(
        x=active_users["Chptrs Created"],
        y=active_users['Chptrs as Contributor'],
        color=active_users['Classification']
    )
    fig.update_layout(
        xaxis_title="Chptrs Created",
        yaxis_title='Chptrs as Contributor',
    )

    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)

    scatter = ax.scatter(
        x=active_users["Chptrs Created"],
        y=active_users['Chptrs as Contributor'],
        c=active_users['Classification'],
        label=active_users['Classification']
    )

    legend = ax.legend(*scatter.legend_elements(),
                loc="lower right", title="User Cluster")

    ax.set_xlabel("Chptrs Created")
    ax.set_ylabel('Chptrs as Contributor')

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Clustering Users")
        st.dataframe(active_users)

    with col2:
        st.write(fig)


    #count of users at each contribution level
    st.subheader("Count of Users by Contribution Number")
    users_agg = users_with_contribution_count.groupby("Count of Contributions").agg({"Count of Contributions": 'count'})
    users_agg = users_agg.rename_axis('Contribution Count')
    users_agg["Count of Users"] = users_agg["Count of Contributions"]
    users_agg = users_agg.drop("Count of Contributions", axis=1)
    st.bar_chart(users_agg)

    #count of owners at each ownership level
    st.subheader("Count of Owners by Chptr Count")
    owners_agg = chptrs_ordered.groupby("Owner").agg({"Owner": 'count'})
    owners_agg = owners_agg.reset_index(drop=True)
    owners_agg = owners_agg.groupby("Owner").agg({"Owner": 'sum'})
    owners_agg = owners_agg.rename_axis('Chptr Count')
    owners_agg["Count of Owners"] = owners_agg["Owner"]
    owners_agg = owners_agg.drop("Owner", axis=1)
    st.bar_chart(owners_agg)

#with tab4:
    #users who have engaged a lot with Chptr
    #st.subheader("Super Users")
    #chptrs_agg = chptrs_ordered.groupby("Owner").agg({"Owner": 'count'})
    #chptrs_agg = chptrs_agg.rename_axis('Chptr Count')
    #chptrs_agg = chptrs_agg.reset_index()
    #super_user = "Kelsey Landrith"
    #st.write("**User:** "+super_user)
    #count = chptrs_agg[(chptrs_agg['Chptr Count']==super_user)]
    #count = count.reset_index()
    #count = count['Owner'][0]
    #st.write("**Chptrs:** "+str(count))
    #users_super = users[(users["User Name"]==super_user)]
    #users_super = users_super.reset_index()
    #users_super_id = users_super["User ID"][0]
    #chptrs_super = chptrs[(chptrs["Chptr Owner"]==users_super_id)]
    #chptrs_super = chptrs_super.reset_index()
    #st.write("**Locations of Chptrs**")
    #for i in range(len(chptrs_super)):
    #    if len(chptrs_super['Location'][i])>0:
    #        st.write("• "+(chptrs_super["Location"][i]))
    #    else:
    #        pass

with tab4:
    #download "transaction" data
    transactions = chptrs_ordered[["Chptr ID", "Chptr Owner","Chptr Name", "Date","Location"]]
    transactions = transactions.sort_values("Date", ascending=True)
    transactions = transactions.reset_index(drop=True)
    st.write("**'Transaction' Data**")
    st.dataframe(transactions)

    transactions_csv = convert_df(transactions)

    st.download_button(
        label="Download 'transaction' data as CSV",
        data=transactions_csv,
        file_name='transactions.csv',
        mime='text/csv',
        )

    #download "key action" data
    key_actions = contributions[['Chptr ID', 'Date', 'Comments', 'Count Likes', 'Contributors']]
    st.write("**'Key Action' (Contribution) Data**")
    st.dataframe(key_actions)

    key_actions_csv = convert_df(key_actions)

    st.download_button(
        label="Download 'key action' data as CSV",
        data=key_actions_csv,
        file_name='key_actions.csv',
        mime='text/csv',
        )

    #download contribution 'action' data
    contributions_actions = contributions_sorted[["Date", "Chptr ID", "User ID", "Chptr Name", "Action Type"]]
    st.write("**Contributions 'Action' Data**")
    st.dataframe(contributions_actions)

    key_actions_contributions = convert_df(contributions_actions)

    st.download_button(
        label="Download contributions as 'key action' data as CSV",
        data=key_actions_contributions,
        file_name='key_actions_contributions.csv',
        mime='text/csv',
        )

    #download comments 'action' data
    st.write("**Comments 'Action' Data**")
    st.dataframe(comments_consolidated)

    key_actions_comments = convert_df(comments_consolidated)

    st.download_button(
        label="Download comments as 'key action' data as CSV",
        data=key_actions_comments,
        file_name='key_actions_comments.csv',
        mime='text/csv',
        )

    #download user data
    st.write("**User Data**")
    st.dataframe(users)

    users_csv = convert_df(users)

    st.download_button(
        label="Download user data as CSV",
        data=users_csv,
        file_name='users.csv',
        mime='text/csv',
        )
    
    #download chptr data
    st.write("**Chptr Data**")
    st.dataframe(chptrs)

    chptrs_csv = convert_df(chptrs)

    st.download_button(
        label="Download chptr data as CSV",
        data=chptrs_csv,
        file_name='chptrs.csv',
        mime='text/csv',
        )

    #download chptr data
    st.write("**Contributions Data**")
    st.dataframe(contributions)

    contributions_csv = convert_df(contributions)

    st.download_button(
        label="Download contributions data as CSV",
        data=contributions_csv,
        file_name='contributions.csv',
        mime='text/csv',
        )
