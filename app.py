
import streamlit as st
from data_loader import load_gold_table, get_storage_options
from deltalake import DeltaTable
import matplotlib.pyplot as plt

# 1. Load Three Gold Tables into DataFrames
facility_df = load_gold_table("facility_analytical")

state_df = load_gold_table("state_summary")

correlation_df = load_gold_table(
    "staffing_readmission_correlation"
)

# 2. Configure the dashboard page
st.set_page_config(
    page_title="Healthcare Metrics Dashboard",
    page_icon="",
    layout="wide"
)

# 3. Display the dashboard title
st.title("Nursing Home Performance Dashboard")
st.write("Healthcare Metrics | Staffing, Capacity & Readmission Performance")

# 4. Define the available metrics
metrics = [
    "Bed Utilization by State",
    "Facility Staffing Distribution by Staff Type",
    "Staffing and Readmissions",
    "Staffing Intensity by State"
]

# 5. Define available nursing staff types
staff_types = [
    "RN",
    "LPN",
    "CNA",
    "Nursing Assistant Trainee",
    "Medication Aide"
]

# 6. Create the sidebar
st.sidebar.header("Explore Metrics")
st.sidebar.write("Select a performance measure and adjust the filters.")

# 7. Create the metric selector
selected_metric = st.sidebar.selectbox(
    "Select a Metric",
    metrics
)

# 8. State filter
available_states = sorted(state_df["state"].dropna().unique())

selected_states = st.sidebar.multiselect(
    "Select State(s)",
    available_states,
    default=available_states
)

# 9. Staff type filter
if selected_metric != "Bed Utilization by State":

    if selected_metric == "Staffing and Readmissions":
        available_staff_types = ["RN", "LPN", "CNA"]
    else:
        available_staff_types = staff_types

    selected_staff_type = st.sidebar.selectbox(
        "Select Staff Type",
        available_staff_types
    )

# 10. Building the Metric Displays

# Building the Bed Utilization Display
if selected_metric == "Bed Utilization by State":

    st.subheader("Bed Utilization Summary")

    # Keep only the states selected in the sidebar
    filtered_state_df = state_df[
        state_df["state"].isin(selected_states)
    ].copy()

    if not filtered_state_df.empty:

        # Combine resident and bed totals across selected states
        total_residents = filtered_state_df[
            "total_average_daily_residents"
        ].sum()

        total_beds = filtered_state_df[
            "total_certified_beds"
        ].sum()

        # Calculate weighted overall bed utilization
        if total_beds > 0:

            overall_utilization = (
                total_residents / total_beds
            ) * 100

            eligible_facilities = filtered_state_df[
                "bed_utilization_facility_count"
            ].sum()

            # Display summary metrics
            col1, col2 = st.columns(2)

            col1.metric(
                "Bed Utilization Across Selected States",
                f"{overall_utilization:.1f}%"
            )

            col2.metric(
                "Eligible Facilities",
                f"{int(eligible_facilities):,}"
            )

            # Display state comparison chart
            st.subheader("Bed Utilization by State")

            # Bar Chart Configuration and Styling
            fig, ax = plt.subplots(figsize=(9, 5), facecolor="#E9EAF0")
            ax.set_facecolor("#E9EAF0")

            bars = ax.bar(
                filtered_state_df["state"],
                filtered_state_df["bed_utilization_percent"],
                color="#E6B655",
                edgecolor="#343A73",
                linewidth=0.8,
                width=0.65
            )

            ax.set_title(
                "Bed Utilization by State",
                fontsize=14,
                fontweight="bold",
                color="#343A73",
                pad=15
            )

            ax.set_xlabel("State", fontsize=11, color="#343A73", labelpad=10)
            ax.set_ylabel("Bed Utilization (%)", fontsize=11, color="#343A73", labelpad=10)

            ax.grid(
                axis="y",
                color="#C8CAD8",
                linestyle="--",
                linewidth=0.8
            )

            ax.set_axisbelow(True)
            ax.tick_params(axis="x", colors="#343A73", rotation=45)
            ax.tick_params(axis="y", colors="#343A73")

            for spine in ax.spines.values():
                spine.set_color("#C8CAD8")

            fig.tight_layout()

            col1, col2, col3 = st.columns([1, 2, 1])

            with col2:
                st.pyplot(fig, width=650)

            plt.close(fig)


        else:
            st.warning(
                "No certified bed data is available for the selected states."
            )

    else:
        st.info(
            "Select at least one state to display bed utilization."
        )
# Buiding the Facility Staffing Distribution by Staff Type Display
elif selected_metric == "Facility Staffing Distribution by Staff Type":

    st.subheader("Facility Staffing Distribution")

    staffing_columns = {
        "RN": "rn_hours_per_resident_day",
        "LPN": "lpn_hours_per_resident_day",
        "CNA": "cna_hours_per_resident_day",
        "Nursing Assistant Trainee": "na_trn_hours_per_resident_day",
        "Medication Aide": "med_aide_hours_per_resident_day"
    }

    selected_column = staffing_columns[selected_staff_type]

    filtered_facility_df = facility_df[
        facility_df["state"].isin(selected_states)
    ].copy()

    filtered_facility_df = filtered_facility_df[
        filtered_facility_df[selected_column].notna()
    ].copy()

    if not filtered_facility_df.empty:

        staffing_values = filtered_facility_df[selected_column]

        median_value = staffing_values.median()
        q1_value = staffing_values.quantile(0.25)
        q3_value = staffing_values.quantile(0.75)
        facility_count = len(staffing_values)

        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Median Hours per Resident Day",
            f"{median_value:.2f}"
        )

        col2.metric(
            "Middle 50% Range",
            f"{q1_value:.2f} – {q3_value:.2f}"
        )

        col3.metric(
            "Eligible Facilities",
            f"{facility_count:,}"
        )

        # Box Plot Configuration and Styling
        fig, ax = plt.subplots(figsize=(9, 5), facecolor="#E5EAF2")
        ax.set_facecolor("#E5EAF2")

        ax.boxplot(
            staffing_values,
            patch_artist=True,
            widths=0.45,
            boxprops={
                "facecolor": "#5B8EAD",
                "edgecolor": "#253B6E",
                "alpha": 0.45,
                "linewidth": 1.5
            },
            medianprops={
                "color": "#E6B655",
                "linewidth": 3
            },
            whiskerprops={
                "color": "#253B6E",
                "linewidth": 1.3
            },
            capprops={
                "color": "#253B6E",
                "linewidth": 1.3
            },
            flierprops={
                "marker": "o",
                "markerfacecolor": "#E6B655",
                "markeredgecolor": "#253B6E",
                "markersize": 5,
                "alpha": 0.7
            }
        )

        ax.set_title(
            f"{selected_staff_type} Staffing Distribution Across Facilities",
            fontsize=14,
            fontweight="bold",
            color="#253B6E",
            pad=15
        )

        ax.set_ylabel(
            "Hours per Resident Day",
            fontsize=11,
            color="#253B6E",
            labelpad=10
        )

        ax.set_xticks([])

        ax.grid(
            axis="y",
            color="#B8C4D6",
            linestyle="--",
            linewidth=0.8
        )

        ax.set_axisbelow(True)
        ax.tick_params(axis="y", colors="#253B6E")

        for spine in ax.spines.values():
            spine.set_color("#B8C4D6")

        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            st.pyplot(fig, width=650)
        plt.close(fig)

    else:
        st.info("Select at least one state to display staffing distribution.")
# Buiding the Staffing and Readmission Correlation Display
elif selected_metric == "Staffing and Readmissions":

    st.subheader("Staffing and Readmission Rates")

    staffing_columns = {
        "RN": "rn_hours_per_resident_day",
        "LPN": "lpn_hours_per_resident_day",
        "CNA": "cna_hours_per_resident_day"
    }

    selected_column = staffing_columns[selected_staff_type]

    filtered_correlation_df = correlation_df[
        correlation_df["state"].isin(selected_states)
    ].copy()

    filtered_correlation_df = filtered_correlation_df.dropna(
        subset=[
            selected_column,
            "fy_2022_risk_standardized_readmission_rate"
        ]
    ).copy()

    if len(filtered_correlation_df) >= 2:

        correlation_value = filtered_correlation_df[
            selected_column
        ].corr(
            filtered_correlation_df[
                "fy_2022_risk_standardized_readmission_rate"
            ]
        )

        facility_count = len(filtered_correlation_df)

        col1, col2 = st.columns(2)
        
        col1.metric(
            "Pearson Correlation",
            f"{correlation_value:.3f}"
        )
        
        col2.metric(
            "Facilities Included",
            f"{facility_count:,}"
        )

        fig, ax = plt.subplots(figsize=(9, 5), facecolor="#E9EAF0")
        ax.set_facecolor("#E9EAF0")

        ax.scatter(
            filtered_correlation_df[selected_column],
            filtered_correlation_df["fy_2022_risk_standardized_readmission_rate"],
            color="#777DAA",
            alpha=0.75,
            edgecolors="#343A73",
            linewidths=0.5,
            s=55
        )

        ax.set_title(
            f"{selected_staff_type} Staffing vs. Readmission Rates",
            fontsize=14,
            fontweight="bold",
            color="#343A73",
            pad=15
        )

        ax.set_xlabel(
            "Staffing Hours per Resident Day",
            fontsize=11,
            color="#343A73",
            labelpad=10
        )

        ax.set_ylabel(
            "Risk-Standardized Readmission Rate",
            fontsize=11,
            color="#343A73",
            labelpad=10
        )

        ax.grid(
            color="#C8CAD8",
            linestyle="--",
            linewidth=0.8
        )

        ax.set_axisbelow(True)
        ax.tick_params(axis="both", colors="#343A73")

        for spine in ax.spines.values():
            spine.set_color("#C8CAD8")

        fig.tight_layout()

        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            st.pyplot(fig, width=650)

        plt.close(fig)

    else:
        st.info(
            "At least two facilities with complete data are required."
        )
# Buiding the Staffing Intensity by State Display
elif selected_metric == "Staffing Intensity by State":

    st.subheader("Staffing Intensity by State")

    staffing_columns = {
        "RN": "rn_hours_per_resident_day",
        "LPN": "lpn_hours_per_resident_day",
        "CNA": "cna_hours_per_resident_day",
        "Nursing Assistant Trainee": "na_trn_hours_per_resident_day",
        "Medication Aide": "med_aide_hours_per_resident_day"
    }

    selected_column = staffing_columns[selected_staff_type]

    filtered_state_df = state_df[
        state_df["state"].isin(selected_states)
    ].copy()

    filtered_state_df = filtered_state_df[
        filtered_state_df[selected_column].notna()
    ].copy()

    if not filtered_state_df.empty:
        staffing_hours_columns = {
            "RN": "total_rn_hours",
            "LPN": "total_lpn_hours",
            "CNA": "total_cna_hours",
            "Nursing Assistant Trainee": "total_na_trn_hours",
            "Medication Aide": "total_med_aide_hours"
        }

        total_staffing_hours = filtered_state_df[
            staffing_hours_columns[selected_staff_type]
        ].sum()

        total_resident_days = filtered_state_df[
            "total_resident_days"
        ].sum()

        if total_resident_days > 0:

            overall_staffing_intensity = (
                total_staffing_hours / total_resident_days
            )

            st.metric(
                f"Overall {selected_staff_type} Hours per Resident Day",
                f"{overall_staffing_intensity:.2f}"
            )

            st.subheader(
                f"{selected_staff_type} Staffing Intensity by State"
            )

            # Bar Chart Configuration and Styling
            fig, ax = plt.subplots(figsize=(9, 5), facecolor="#E9EAF0")
            ax.set_facecolor("#E9EAF0")

            ax.bar(
                filtered_state_df["state"],
                filtered_state_df[selected_column],
                color="#E6B655",
                edgecolor="#343A73",
                linewidth=0.8,
                width=0.65
            )

            ax.set_title(
                f"{selected_staff_type} Staffing Intensity by State",
                fontsize=14,
                fontweight="bold",
                color="#343A73",
                pad=15
            )

            ax.set_xlabel("State", fontsize=11, color="#343A73", labelpad=10)
            ax.set_ylabel(
                "Hours per Resident Day",
                fontsize=11,
                color="#343A73",
                labelpad=10
            )

            ax.grid(
                axis="y",
                color="#C8CAD8",
                linestyle="--",
                linewidth=0.8
            )

            ax.set_axisbelow(True)
            ax.tick_params(axis="x", colors="#343A73", rotation=45)
            ax.tick_params(axis="y", colors="#343A73")

            for spine in ax.spines.values():
                spine.set_color("#C8CAD8")

            fig.tight_layout()

            col1, col2, col3 = st.columns([1, 2, 1])

            with col2:
                st.pyplot(fig, width=650)

            plt.close(fig)


        else:
            st.info("No resident-day data is available for the selected states.")
    else:
        st.info(
             "Select at least one state to display state staffing intensity."
        )
    

# Display the selected metric
# st.header(selected_metric)
