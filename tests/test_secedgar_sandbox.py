import os
import sys
import subprocess

def install_and_add_to_path():
    pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '.local_packages'))
    os.makedirs(pkg_dir, exist_ok=True)
    sys.path.insert(0, pkg_dir)
    
    try:
        import secedgar
    except ImportError:
        print("Installing secedgar to local path...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", pkg_dir, "secedgar"])

install_and_add_to_path()

from secedgar import filings, FilingType

def test_secedgar():
    print("Testing secedgar download... AAPL 10-Q (1 document)")
    try:
        my_filings = filings(cik_lookup="AAPL",
                             filing_type=FilingType.FILING_10Q,
                             count=1,
                             user_agent="Google_Deepmind_Tester (test@google.com)")
        save_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '__secedgar_data__'))
        print(f"Saving to {save_path}")
        my_filings.save(save_path)
        print("Download complete. Checking downloaded files:")
        for root, dirs, files in os.walk(save_path):
            for file in files:
                if file.endswith('.txt') or file.endswith('.htm'):
                    filepath = os.path.join(root, file)
                    print(f"Found downloaded file: {filepath}")
                    print(f"File size: {os.path.getsize(filepath) / 1024:.2f} KB")
                    
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read(50000)
                        print(f"First 100 bytes: {content[:100]!r}...")
                        
                        has_mda = 'Management' in content or 'MD&A' in content
                        has_risk = 'Risk' in content
                        print(f"Contains 'Management' keyword: {has_mda}")
                        print(f"Contains 'Risk' keyword: {has_risk}")
                        print("---")
                        print("Conclusion: As shown, the file is raw text/HTML without parsed structure.")
        print("\nTest completed successfully.")
    except Exception as e:
        print(f"Error occurred during secedgar test: {e}")

if __name__ == "__main__":
    test_secedgar()
