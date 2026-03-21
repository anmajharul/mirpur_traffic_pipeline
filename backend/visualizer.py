import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os

def plot_traffic_profile():
    # ১. সিএসভি ফাইল লোড করা
    csv_path = "backend/traffic_data_backup.csv"
    if not os.path.exists(csv_path):
        print("❌ CSV ফাইলটি খুঁজে পাওয়া যায়নি!")
        return

    df = pd.read_csv(csv_path)
    
    # ২. টাইমস্ট্যাম্প প্রসেসিং এবং BD Time (UTC+6) এ কনভার্শন
    # প্রথমে স্ট্রিং থেকে প্যান্ডাস ডেটটাইম অবজেক্টে রূপান্তর
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    # টাইমজোন হ্যান্ডলিং (UTC থেকে Asia/Dhaka)
    try:
        # যদি ডেটাবেস থেকে আসা টাইমস্ট্যাম্পে টাইমজোন না থাকে (Naive), তবে প্রথমে UTC ধরে নিতে হবে
        if df['created_at'].dt.tz is None:
            df['created_at'] = df['created_at'].dt.tz_localize('UTC').dt.tz_convert('Asia/Dhaka')
        else:
            # যদি আগে থেকেই টাইমজোন থাকে (Aware), তবে সরাসরি ঢাকায় কনভার্ট করো
            df['created_at'] = df['created_at'].dt.tz_convert('Asia/Dhaka')
    except Exception as e:
        print(f"⚠️ Timezone Conversion Warning: {e}")

    df = df.sort_values('created_at')

    # ৩. গ্রাফের স্টাইল সেটআপ
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")

    # ৪. প্রতিটি ডিরেকশনের জন্য আলাদা লাইন প্লট করা
    directions = df['direction'].unique()
    for direction in directions:
        subset = df[df['direction'] == direction]
        plt.plot(subset['created_at'], subset['speed_kmh'], marker='o', label=direction, linewidth=2)

    # ৫. গ্রাফের লেবেলিং (রিসার্চ স্ট্যান্ডার্ড)
    plt.title("Real-time Traffic Speed Profile: Mirpur-10 Circle", fontsize=16, fontweight='bold')
    plt.xlabel("Time of Day (Bangladesh Standard Time)", fontsize=12)
    plt.ylabel("Speed (km/h)", fontsize=12)
    plt.legend(title="Corridors", bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # এক্স-অ্যাক্সিস (X-axis) এ সময় দেখানোর ফরম্যাট ঠিক করা (যেমন: 14:30)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.xticks(rotation=45)
    plt.tight_layout()

    # ৬. গ্রাফটি ইমেজ হিসেবে সেভ করা
    os.makedirs("backend", exist_ok=True) # যদি ফোল্ডার না থাকে তবে ক্র্যাশ এড়াতে তৈরি করে নেবে
    plt.savefig("backend/traffic_speed_profile.png", dpi=300)
    print("✅ গ্রাফটি 'backend/traffic_speed_profile.png' নামে সেভ হয়েছে!")
    plt.show()

if __name__ == "__main__":
    plot_traffic_profile()