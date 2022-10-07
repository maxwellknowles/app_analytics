#requirements
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore
from datetime import datetime
from st_aggrid import AgGrid
import streamlit as st
from geopy.geocoders import Nominatim
import statistics
import numpy as np
import altair as alt

#page setup
st.set_page_config(page_title="Chptr Analytics", page_icon=":rocket:", layout="wide",initial_sidebar_state="expanded")

JSON_DATA = {'key':st.secrets['google_key_file']}

#functions
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def connect_to_firestore():
    #connecting to firebase
    cred = credentials.Certificate(JSON_DATA["key"])
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://console.firebase.google.com/u/2/project/chptr-b101d/firestore/data'
    })
connect_to_firestore()
    
db = firestore.client()


#users collection to dataframe
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
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
        email_allowed = users["allowsReceiveEmail"][i]
        state = users["state"][i]
        chptrIds = users["chptrIds"][i]
        number_chptrs = len(users["chptrIds"][i])
        tup = (selectedChptrID, 
            user_id,
            name,
            pendingChptrRequests,
            email_allowed,
            state,
            chptrIds,
            number_chptrs
            )
        l.append(tup)
    users_consolidated = pd.DataFrame(l,columns=["Selected Chptr ID",
                                                "User ID",
                                                "User Name",
                                                "Pending Chapter Requests",
                                                "Email Allowed",
                                                "State",
                                                "Chptr IDs",
                                                "Number of Chptrs"
                                                ])
    return users_consolidated

#chptrs collection to dataframe
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
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
                                                "Pending Requests",
                                                "Contributors",
                                                "Number of Contributors"])
    return chptrs_consolidated

#contributions collection to dataframe
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def get_contributions():
    contributions = list(db.collection(u'contributions').stream())
    contributions_dict = list(map(lambda x: x.to_dict(), contributions))
    contributions = pd.DataFrame(contributions_dict)
    l=[]
    for i in range(len(contributions)):
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
        tup = (chptrId, 
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
        contributions_consolidated = pd.DataFrame(l,columns=["Chptr ID", 
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

#START OF STREAMLIT PAGE
st.header("Chptr Analytics")
st.write("**Key Assumptions and Notes**")
st.write("• Chptr has yet to make a push on marketing")
st.write("• Chptr has yet to monetize Chptr creation, but that is what should present a 'transaction' event")
st.write("• Contributions to Chptrs represent engagement, but are not considered monetizable actions, now or in the future")
st.subheader("High Level Stats")
col1, col2, col3, col4 = st.columns(4)
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
    st.metric("Categories per Contribution",round(statistics.mean(contributions["Count Categories"]),2))
    #st.metric("Comments per Contribution",round(statistics.mean(contributions["Comments"]),2))

with col4:
    st.write("**RATINGS**")
    st.metric("Five-Star Reviews", "100%", "153 reviews")

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

tab1, tab2, tab3, tab4 = st.tabs(["Acquisition", "Activation", "Retention", "Download Data"])

with tab1:
    #chptrs over time
    st.subheader("Chptr Publications by Month")
    chptrs_ordered_publications = chptrs_ordered.groupby("Month").agg({"Month": 'count'})
    chptrs_ordered_publications = chptrs_ordered_publications.rename_axis('Month Number')
    chptrs_ordered_publications["Chptrs"] = chptrs_ordered_publications["Month"]
    chptrs_ordered_publications = chptrs_ordered_publications.drop("Month", axis=1)
    st.bar_chart(chptrs_ordered_publications)

    chptrs_passing_data = chptrs_ordered[["Chptr ID", "Birthday","Passing Date", "Date"]]
    chptrs_passing_data = chptrs_passing_data[chptrs_passing_data['Passing Date'].notna()]
    chptrs_passing_data = chptrs_passing_data[chptrs_passing_data['Date'].notna()]
    chptrs_passing_data = chptrs_passing_data.reset_index(drop=True)
    passing_delta = []
    for i in range(len(chptrs_passing_data)):
        chptr_id=chptrs_passing_data["Chptr ID"][i]
        passing_date=str(chptrs_passing_data["Passing Date"][i]).split("T")[0]
        passing_date=datetime.strptime(passing_date, '%Y-%m-%d').year
        chptr_date=str(chptrs_passing_data["Date"][i]).split("T")[0]
        chptr_date = datetime.strptime(chptr_date, '%Y-%m-%d').year
        delta=chptr_date-passing_date
        tup=(chptr_id, delta)
        passing_delta.append(tup)
    chptrs_passing=pd.DataFrame(passing_delta, columns=['Chptr ID', "Years Passed Before Chptr Creation"])
    chptrs_passing=chptrs_passing[(chptrs_passing["Years Passed Before Chptr Creation"]>=0)]
    
    chptrs_agg = chptrs_passing.groupby("Years Passed Before Chptr Creation").agg({"Years Passed Before Chptr Creation": 'count'})
    chptrs_agg["Count of Chptrs"] = chptrs_agg["Years Passed Before Chptr Creation"]
    chptrs_agg = chptrs_agg.drop("Years Passed Before Chptr Creation", axis=1)
    chptrs_agg = chptrs_agg.rename_axis("Years Passed Before Chptr Creation")
    chptrs_agg = chptrs_agg.reset_index()
    c = alt.Chart(chptrs_agg).mark_circle().encode(
        x="Years Passed Before Chptr Creation", y="Count of Chptrs", size="Count of Chptrs", color="Count of Chptrs", tooltip=["Years Passed Before Chptr Creation", "Count of Chptrs"])

    st.subheader("Chptr Count and Years Relative to Passing")
    st.altair_chart(c, use_container_width=True)

    #graph of locations
    st.subheader("Chptrs by City")
    chptrs_lat_lon = chptrs[['Lat','Lon']]
    chptrs_lat_lon=chptrs_lat_lon[chptrs_lat_lon.Lat.notnull()]
    chptrs_lat_lon['lat']=chptrs_lat_lon['Lat']
    chptrs_lat_lon['lon']=chptrs_lat_lon['Lon']
    st.map(chptrs_lat_lon)

with tab2:
    #count of users at each contribution level
    st.subheader("Count of Users by Contribution Number")
    users_agg = users_with_contribution_count.groupby("Count of Contributions").agg({"Count of Contributions": 'count'})
    users_agg = users_agg.rename_axis('Contribution Count')
    users_agg["Count of Users"] = users_agg["Count of Contributions"]
    users_agg = users_agg.drop("Count of Contributions", axis=1)
    st.bar_chart(users_agg)

    #contributions per chptr over time
    st.subheader("Avg Count of Contributors per Chptr by Month")
    chptrs_ordered_contributors = chptrs_ordered[["Month","Count Contributors"]]
    chptrs_ordered_contributors = chptrs_ordered_contributors.groupby("Month").agg({"Count Contributors": 'mean'})
    st.bar_chart(chptrs_ordered_contributors)

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

    #graph of category usage
    st.subheader("Category Popularity")
    st.bar_chart(category_performance)

with tab3:
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
    #download data
    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    #download "transaction" data
    transactions = chptrs_ordered[["Chptr ID", "Chptr Owner","Date","Location"]]
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
