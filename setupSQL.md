# Setting Up MySQL on macOS and Windows

## Prerequisites
- Administrator privileges on your machine.
- Download the MySQL installer or package from the [MySQL official website](https://dev.mysql.com/).

---

## Setting Up MySQL on macOS

1. **Download MySQL**
   - Visit the [MySQL Community Downloads page](https://dev.mysql.com/downloads/mysql/).
   - Choose the macOS version and download the `.dmg` file.

2. **Install MySQL**
   - Open the downloaded `.dmg` file and run the installer.
   - Follow the installation prompts (default settings are sufficient for most cases).
   - Set a root password during the setup and store it securely.

3. **Add MySQL to PATH**
   - Open your Terminal.
   - Add the following line to your shell configuration file (e.g., `.zshrc`, `.bash_profile`, or `.bashrc`):
     ```bash
     export PATH="/usr/local/mysql/bin:$PATH"
     ```
   - Save the file and run:
     ```bash
     source ~/.zshrc   # Use the appropriate file for your shell
     ```

4. **Start MySQL Service**
   - Start MySQL using macOS System Preferences or via Terminal:
     ```bash
     sudo /usr/local/mysql/support-files/mysql.server start
     ```

5. **Verify Installation**
   - Open Terminal and type:
     ```bash
     mysql -u root -p
     ```
   - Enter your root password to access the MySQL shell.

---

## Setting Up MySQL on Windows

1. **Download MySQL**
   - Visit the [MySQL Community Downloads page](https://dev.mysql.com/downloads/mysql/).
   - Choose the Windows version and download the MySQL Installer.

2. **Install MySQL**
   - Run the MySQL Installer and choose "Server Only" or "Custom" installation.
   - Follow the prompts and configure the root password during the setup.

3. **Add MySQL to PATH**
   - If not done during installation, manually add MySQL to your system PATH:
     - Right-click on "This PC" or "My Computer" and select "Properties."
     - Go to "Advanced System Settings" > "Environment Variables."
     - Under "System Variables," find `Path`, click "Edit," and add the path to your MySQL `bin` directory (e.g., `C:\Program Files\MySQL\MySQL Server 8.0\bin`).

4. **Start MySQL Service**
   - Open the Services app (`services.msc`).
   - Locate "MySQL," right-click, and select "Start."

5. **Verify Installation**
   - Open Command Prompt and type:
     ```cmd
     mysql -u root -p
     ```
   - Enter your root password to access the MySQL shell.

---

## Common Post-Installation Steps
- Secure your MySQL installation by running:
  ```bash
  mysql_secure_installation
