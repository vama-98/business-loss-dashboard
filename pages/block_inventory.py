import streamlit as st
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import io

# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(page_title="Block Inventory", layout="wide")
st.title("📦 Block Inventory Simulation & Notification Tool")

st.markdown("""
This tool allows you to:
- Simulate blocking impact on inventory  
- Upload CSV/XLSX SKU files  
- Notify SCM team directly via email  
""")

# -------------------------------
# SIMULATION SECTION
# -------------------------------
st.subheader("🧮 Inventory Block Simulation")

department = st.selectbox("Select Department", ["D2C", "Marketplace", "Offline"])
product = st.text_input("Enter Product / SKU Name")
current_inventory = st.number_input("Current Inventory", min_value=0, value=1000, step=100)
drr = st.number_input("Daily Run Rate (DRR)", min_value=0.0, value=50.0, step=1.0)
block_qty = st.number_input("Quantity to Block", min_value=0, value=0, step=10)

simulate_btn = st.button("🔍 Simulate Impact")

if simulate_btn:
    remaining = current_inventory - block_qty
    new_doh = remaining / drr if drr > 0 else 0
    st.write(f"📦 **Available after blocking:** {remaining}")
    st.write(f"⏳ **New DOH (Days of Holding):** {new_doh:.1f} days")

    if new_doh < 15:
        st.warning("⚠️ Risky! Blocking will drop DOH below 15 days — potential OOS risk.")
    else:
        st.success("✅ Safe to block — inventory remains healthy.")

# -------------------------------
# FILE UPLOAD
# -------------------------------
st.markdown("---")
st.subheader("📤 Upload SKU File for Blocking")

uploaded_file = st.file_uploader("Upload CSV or Excel file", type=["csv", "xlsx"])
if uploaded_file:
    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        st.success("✅ File uploaded successfully!")
        st.dataframe(df.head())
    except Exception as e:
        st.error(f"Error reading file: {e}")

# -------------------------------
# EMAIL SENDING
# -------------------------------
st.markdown("---")
st.subheader("📧 Send Block Inventory Request")

if st.button("📨 Send Block Request Email"):
    try:
        email_cfg = st.secrets["email"]
        sender = email_cfg["sender"]
        password = email_cfg["password"]
        recipients = email_cfg["recipients"]

        remaining = current_inventory - block_qty
        new_doh = remaining / drr if drr > 0 else 0

        subject = f"🚫 Block Inventory Request | {department} | {product or 'SKU'}"

        body = f"""
Hello Team,

A block inventory request has been initiated via the Pilgrim Automation Dashboard.

📋 **Details:**
- Department: {department}
- Product/SKU: {product or 'N/A'}
- Block Quantity: {block_qty}
- Current Inventory: {current_inventory}
- DRR: {drr}
- Remaining DOH: {new_doh:.1f} days

Please find the uploaded SKU file attached for reference.

Best regards,  
Pilgrim Automation Dashboard  
"""

        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Attach uploaded file
        if uploaded_file:
            uploaded_file.seek(0)
            part = MIMEBase("application", "octet-stream")
            part.set_payload(uploaded_file.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={uploaded_file.name}")
            msg.attach(part)

        # Send email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)

        st.success(f"✅ Block inventory request sent successfully to {', '.join(recipients)}")

    except Exception as e:
        st.error(f"❌ Failed to send email: {e}")
