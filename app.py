import datetime

import streamlit as st
import pandas as pd
import logging
from sqlalchemy import select
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
import sqlalchemy as sa
from enum import Enum
import math
import time
# these 2 are the requirements because using pyplot can cause memory leak
from matplotlib.figure import Figure
from matplotlib.patches import Circle

st.set_page_config(
    page_title="Recently Birdnest NDZ Violators",
    layout="wide",
)

Base = declarative_base()

logging.getLogger().setLevel(logging.WARNING)

CENTER_X = 250000
CENTER_Y = 250000
RADIUS = 100000


class Query(Enum):
    DRONES = "drones"
    PILOTS = 'pilots'


class ViolatedPilots(Base):
    __tablename__ = "violated_pilots"
    pilot_id = sa.Column(sa.VARCHAR, primary_key=True, index=True)
    first_name = sa.Column(sa.VARCHAR)
    last_name = sa.Column(sa.VARCHAR)
    phone_number = sa.Column(sa.VARCHAR)
    email = sa.Column(sa.VARCHAR)
    created_dt = sa.Column(sa.DATETIME)
    last_violation_at = sa.Column(sa.DATETIME, index=True)
    last_violation_x = sa.Column(sa.FLOAT)
    last_violation_y = sa.Column(sa.FLOAT)
    nearest_violation_x = sa.Column(sa.FLOAT)
    nearest_violation_y = sa.Column(sa.FLOAT)

    def to_dict(self):
        return {
            'pilot_id': self.pilot_id,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'phone_number': self.phone_number,
            'email': self.email,
            'created_dt': self.created_dt,
            'last_violation_at': self.last_violation_at,
            'last_violation_x': self.last_violation_x,
            'last_violation_y': self.last_violation_y,
            'nearest_violation_x': self.nearest_violation_x,
            'nearest_violation_y': self.nearest_violation_y
        }


class Drones(Base):
    __tablename__ = "drones"
    serial_number = sa.Column(sa.VARCHAR, primary_key=True)
    manufacturer = sa.Column(sa.VARCHAR)
    mac = sa.Column(sa.VARCHAR)
    ipv4 = sa.Column(sa.VARCHAR)
    ipv6 = sa.Column(sa.VARCHAR)
    firmware = sa.Column(sa.VARCHAR)
    position_x = sa.Column(sa.FLOAT)
    position_y = sa.Column(sa.FLOAT)
    altitude = sa.Column(sa.FLOAT)
    is_violating_ndz = sa.Column(sa.BOOLEAN)
    violated_pilot_id = sa.Column(sa.INTEGER, ForeignKey("violated_pilots.pilot_id"),
                                  unique=True, index=True, nullable=True)
    created_at = sa.Column(sa.DATETIME)
    updated_at = sa.Column(sa.DATETIME, index=True)
    # this relationship basically delete associated pilot once drone is deleted
    violated_pilot = relationship("ViolatedPilots", cascade="all, delete")

    def to_dict(self):
        return {
            'serial_number': self.serial_number,
            'manufacturer': self.manufacturer,
            'mac': self.mac,
            'ipv4': self.ipv4,
            'ipv6': self.ipv6,
            'firmware': self.firmware,
            'position_x': self.position_x,
            'position_y': self.position_y,
            'altitude': self.altitude,
            'is_violating_ndz': self.is_violating_ndz,
            'violated_pilot_id': self.violated_pilot_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }


# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    nengine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        st.secrets['postgres']['user'],
        st.secrets['postgres']['password'],
        st.secrets['postgres']['host'],
        st.secrets['postgres']['port'],
        st.secrets['postgres']['dbname'],
    ))
    return nengine  # psycopg2.connect(**st.secrets["postgres"])


engine = init_connection()


def string_to_stmt_factory(q: Query):
    if q == Query.DRONES:
        return select(Drones)
    elif q == Query.PILOTS:
        return select(ViolatedPilots)
    raise Exception("Invalid Query")


# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 seconds.
@st.experimental_memo(ttl=10)
def run_query(query: Query):
    """
    function to run query require hashable input and output
    function only usable for Drones or ViolatedPilots because they have to_dict
    :param query:
    :return:
    """
    stmt = string_to_stmt_factory(query)
    session = Session(engine)
    results = session.execute(stmt).scalars().all()
    session.close()
    results = list(map(lambda x: x.to_dict(), results))  # turn to dict to be serializable
    return results


def highlight_not_null(s, column):
    """
    for apply method on Dataframe Styler to highlight not null
    :param s:
    :param column:
    :return:
    """
    is_not_null = pd.Series(data=False, index=s.index)
    is_not_null[column] = ~pd.isna(s.loc[column])
    return ['background-color: darkred' if is_not_null.any() else '' for v in is_not_null]


def distance_from_nest_in_meter(x, y):
    """
    expect x and y to come in x meter * 1000, hence require divide by 1000 to become meter
    :param x:
    :param y:
    :return:
    """
    return math.sqrt((x - CENTER_X) ** 2 + (y - CENTER_Y) ** 2) / 1000


place_holder = st.empty()  # component required for automated updating and layout

# Main Application Loop
while True:
    # Fetch Data
    drones = run_query(Query.DRONES)
    pilots = run_query(Query.PILOTS)

    # Processing of drones information
    drones_df = pd.DataFrame(drones)
    drones_df['current_distance_from_nest_in_meter'] = drones_df.apply(
        lambda row: distance_from_nest_in_meter(row['position_x'],
                                                row['position_y']), axis=1)
    drones_display_columns = ['serial_number',
                              'current_distance_from_nest_in_meter',
                              'position_x',
                              'position_y',
                              'altitude',
                              'is_violating_ndz',
                              'violated_pilot_id',
                              'updated_at']
    drones_hide_columns = list(set(drones_df.columns).difference(set(drones_display_columns)))

    # Processing of pilot information
    pilots_df = pd.DataFrame(pilots)
    pilots_df['last_violation_distance_in_meter'] = pilots_df.apply(
        lambda row: distance_from_nest_in_meter(row['last_violation_x'],
                                                row['last_violation_y']), axis=1)
    pilots_df['nearest_violation_distance_in_meter'] = pilots_df.apply(
        lambda row: distance_from_nest_in_meter(row['nearest_violation_x'],
                                                row['nearest_violation_y']),
        axis=1)
    pilots_display_columns = ['pilot_id', 'first_name', 'last_name', 'phone_number', 'email',
                              'nearest_violation_distance_in_meter', 'last_violation_distance_in_meter',
                              'last_violation_at',
                              'last_violation_x', 'last_violation_y',
                              'nearest_violation_x', 'nearest_violation_y']
    pilots_hide_columns = list(set(pilots_df.columns).difference(set(pilots_display_columns)))

    # for displaying drones locations
    bad_drones = drones_df[pd.isna(drones_df['violated_pilot_id'])][['position_x', 'position_y', 'is_violating_ndz']]
    bad_currently_violating = bad_drones[bad_drones['is_violating_ndz']][['position_x', 'position_y']]
    bad_not_currently_violating = bad_drones[~bad_drones['is_violating_ndz']][['position_x', 'position_y']]
    good_drones = drones_df[~pd.isna(drones_df['violated_pilot_id'])][['position_x', 'position_y']]

    # Write out the tables

    with place_holder:
        with place_holder.container():
            # Write title and subheaders
            st.title('Recent Birdnest NDZ Violators')
            st.markdown('Last Data Update: {}'.format(datetime.datetime.now().isoformat()))
            st.markdown('Data is updated every few seconds.')
            st.markdown('Use below tabs to switch between different viewings.')
            tab1, tab2, tab3, tab4 = st.tabs(["Pilots", "Drones", "Drone Positions", "Violation Positions"])

            # Create pilot dataframe tab
            with tab1:
                st.markdown("### Pilots who recently violate NDZ")
                st.markdown("Table indicates details of those who recently violate NDZ (10 minutes).")
                pilots_view = pilots_df[pilots_display_columns].sort_values("last_violation_at").reset_index(drop=True)
                st.dataframe(pilots_view.style.format('{:.0f}',
                                                      subset=['last_violation_x',
                                                              'last_violation_y',
                                                              'last_violation_distance_in_meter',
                                                              'nearest_violation_x',
                                                              'nearest_violation_y',
                                                              'nearest_violation_distance_in_meter']),
                             use_container_width=True)

            # Create drone dataframe tab
            with tab2:
                st.markdown("### Drones Detected")
                st.markdown("Red rows indicate drones whose pilots have recently violated NDZ.")
                drones_df = drones_df.sort_values(["updated_at"], ascending=False)
                drones_view = drones_df[drones_display_columns].reset_index(drop=True)
                st.dataframe(
                    drones_view.style.apply(highlight_not_null, column=['violated_pilot_id'], axis=1).format(
                        '{:.0f}',
                        subset=['position_x',
                                'position_y',
                                'altitude',
                                'current_distance_from_nest_in_meter']
                    ),
                    use_container_width=True)

            # plt.close('all')  # close so that the plot dont get overwrite and cause memory overflow (potentially)

            # Create current positions plot tab
            with tab3:
                fig = Figure()  # instantiate Figure for plotting
                ax = fig.subplots()
                ndz_circle = Circle((CENTER_X, CENTER_Y), RADIUS, color='b', fill=False)
                ax.add_patch(ndz_circle)
                ax.scatter(bad_currently_violating['position_x'], bad_currently_violating['position_y'],
                           marker='x', c='red')
                ax.scatter(bad_not_currently_violating['position_x'], bad_not_currently_violating['position_y'],
                           marker='^', c='orange')
                ax.scatter(good_drones['position_x'], good_drones['position_y'],
                           marker='o', c='green')
                ax.legend(
                    ['NDZ', 'Currently Violating NDZ', 'Recently Violate NDZ', 'Have not violate NDZ recently'],
                    bbox_to_anchor=(1.04, 1), borderaxespad=0)
                fig.subplots_adjust(right=0.8)
                ax.tick_params(axis='both', which='major', labelsize=10)
                ax.set_title("Drone Positions")
                st.pyplot(fig)

            # Create all violation positions tab
            with tab4:
                fig = Figure()  # instantiate Figure for plotting
                ax = fig.subplots()
                ndz_circle = Circle((CENTER_X, CENTER_Y), RADIUS, color='b', fill=False)
                ax.add_patch(ndz_circle)
                ax.scatter(pilots_view['nearest_violation_x'], pilots_view['nearest_violation_y'],
                           marker='x', c='red')
                ax.scatter(pilots_view['last_violation_x'], pilots_view['last_violation_y'],
                           marker='^', c='orange')
                ax.legend(
                    ['NDZ', 'Nearest Violations', 'Last Violations'],
                    bbox_to_anchor=(1.04, 1), borderaxespad=0)
                fig.subplots_adjust(right=0.8)
                ax.tick_params(axis='both', which='major', labelsize=7)
                ax.set_title("Violation Positions")
                st.pyplot(fig)
    # sleep for 3 seconds before rerunning this loop to automatically update data without refresh
    time.sleep(3)
