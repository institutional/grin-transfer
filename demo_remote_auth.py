#!/usr/bin/env python3
"""
Interactive demonstration of remote authentication flow.
This script simulates what users would see when using --remote-auth flag.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from grin_to_s3.auth import detect_remote_shell
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root and have dependencies installed")
    sys.exit(1)


def demo_detection():
    """Demonstrate environment detection."""
    print("🔍 Remote Shell Environment Detection")
    print("=" * 45)

    result = detect_remote_shell()
    if result:
        print("✅ Remote shell environment detected!")
        print("   The system will automatically use manual authorization mode.")
    else:
        print("❌ Local environment detected.")
        print("   Use --remote-auth flag to force manual authorization mode.")

    print("\nDetection criteria:")
    print("• SSH indicators: SSH_CLIENT, SSH_TTY, SSH_CONNECTION environment variables")
    print("• Terminal multiplexers: tmux, screen")
    print("• Cloud platforms: AWS, GCP, Azure (when no DISPLAY)")
    print("• Manual override: --remote-auth flag")
    print()


def demo_manual_flow():
    """Demonstrate what the manual flow would look like."""
    print("🔐 Manual Authorization Flow Demo")
    print("=" * 45)

    print("When you run: python grin.py auth setup --remote-auth")
    print("You would see something like this:")
    print()

    # Simulate the output
    print("="*60)
    print("REMOTE SHELL OAUTH2 SETUP")
    print("="*60)
    print("Remote shell detected - manual authorization required")
    print()
    print("1. Open this URL in a browser on your LOCAL machine:")
    print("   https://accounts.google.com/o/oauth2/auth?client_id=your-client-id...")
    print()
    print("2. Complete Google authentication with your GRIN account")
    print("3. Google will display an authorization code")
    print("4. Copy the code and paste it below")
    print("="*60)
    print()
    print("Authorization code: [USER ENTERS CODE HERE]")
    print()
    print("✅ Authentication successful! Credentials saved.")
    print()


def demo_comparison():
    """Show comparison of all three auth modes."""
    print("📊 Authentication Modes Comparison")
    print("=" * 45)

    modes = [
        {
            "name": "Local Desktop",
            "detection": "No SSH/remote indicators",
            "behavior": "Opens browser automatically, localhost callback",
            "user_action": "Just click 'Allow' in auto-opened browser"
        },
        {
            "name": "Docker Container",
            "detection": "Docker environment detected",
            "behavior": "Port forwarding setup, manual URL visit",
            "user_action": "Visit provided URL, browser redirects to localhost"
        },
        {
            "name": "Remote Shell/SSH",
            "detection": "SSH vars, tmux/screen, or cloud platform",
            "behavior": "Manual authorization code flow",
            "user_action": "Visit URL on local machine, copy/paste code"
        }
    ]

    for i, mode in enumerate(modes, 1):
        print(f"{i}. {mode['name']}:")
        print(f"   Detection: {mode['detection']}")
        print(f"   Behavior: {mode['behavior']}")
        print(f"   User action: {mode['user_action']}")
        print()


def main():
    """Run the demonstration."""
    print("🚀 Remote Authentication Flow Demonstration")
    print("=" * 55)
    print("This demo shows how the enhanced OAuth2 flow works")
    print("for remote shell environments (SSH, cloud instances).")
    print()

    demo_detection()
    demo_manual_flow()
    demo_comparison()

    print("🎯 Key Benefits:")
    print("• No SSH port forwarding setup required")
    print("• Works seamlessly on cloud instances")
    print("• Clear step-by-step user guidance")
    print("• Automatic environment detection")
    print("• Full backward compatibility")
    print()

    print("📝 To test with real OAuth2 credentials:")
    print("1. Set up client_secret.json (see README)")
    print("2. Run: python grin.py auth setup --remote-auth")
    print("3. Follow the interactive prompts")


if __name__ == "__main__":
    main()
