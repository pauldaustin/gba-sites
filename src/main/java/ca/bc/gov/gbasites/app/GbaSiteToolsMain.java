package ca.bc.gov.gbasites.app;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Date;

import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.WindowConstants;

import org.jdesktop.swingx.VerticalLayout;
import org.jeometry.common.date.Dates;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaConfig;
import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.controller.GbaLogController;
import ca.bc.gov.gba.itn.GbaItnDatabase;
import ca.bc.gov.gbasites.controller.GbaSiteController;
import ca.bc.gov.gbasites.load.ImportSites;

import com.revolsys.log.LogbackUtil;
import com.revolsys.process.JavaProcess;
import com.revolsys.swing.Borders;
import com.revolsys.swing.Icons;
import com.revolsys.swing.SwingUtil;
import com.revolsys.swing.TabbedPane;
import com.revolsys.swing.action.RunnableAction;
import com.revolsys.swing.action.enablecheck.BooleanEnableCheck;
import com.revolsys.swing.component.BasePanel;
import com.revolsys.swing.logging.LoggingTableModel;
import com.revolsys.swing.menu.MenuFactory;
import com.revolsys.swing.parallel.BackgroundTaskTableModel;
import com.revolsys.swing.parallel.BaseMain;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.swing.scripting.ScriptRunner;

public class GbaSiteToolsMain extends BaseMain {
  public static void main(final String[] args) {
    System.setProperty("awt.useSystemAAFontSettings", "lcd");
    System.setProperty("com.apple.mrj.application.apple.menu.about.name", "GBA Site Tools");

    run(GbaSiteToolsMain.class, args);
  }

  private JFrame frame;

  public GbaSiteToolsMain() {
    super("GBA Site Tools");
  }

  public void exit() {
    System.exit(0);
  }

  protected void newMenu(final JMenuBar menuBar, final MenuFactory menu) {
    final JMenu fileMenu = menu.newComponent();
    menuBar.add(fileMenu);
  }

  protected JMenuBar newMenuBar() {
    final JMenuBar menuBar = new JMenuBar();
    this.frame.setJMenuBar(menuBar);

    newMenuFile(menuBar);
    newMenuTools(menuBar);

    return menuBar;
  }

  protected MenuFactory newMenuFile(final JMenuBar menuBar) {
    final MenuFactory file = new MenuFactory("File");

    file.addMenuItemTitleIcon("exit", "Exit", null, this::exit);
    newMenu(menuBar, file);
    return file;
  }

  protected MenuFactory newMenuTools(final JMenuBar menuBar) {
    final MenuFactory tools = new MenuFactory("Tools");

    tools.addMenuItem("script", "Run Script...", "Run Script", "script_go", () -> {
      final JavaProcess javaProcess = GbaController.newJavaProcess();
      final File logDirectory = GbaLogController.getUserLogDirectory();
      ScriptRunner.runScriptProcess(this.frame, logDirectory, javaProcess);
    });
    newMenu(menuBar, tools);
    return tools;
  }

  private void newPanelTasks(final JPanel container) {
    final BasePanel panel = new BasePanel(new FlowLayout(FlowLayout.LEFT));
    Borders.titled(panel, "Tasks");
    container.add(panel);

    final BooleanEnableCheck backupEnableCheck = new BooleanEnableCheck();
    panel.add(RunnableAction.newButton("Import Sites", backupEnableCheck,
      () -> runProcess(ImportSites.class, backupEnableCheck)));
  }

  @Override
  protected void preRunDo() {
    setMacDockIcon(Icons.getImage("gba_tools_icon_32"));
    final long time = System.currentTimeMillis();

    final java.nio.file.Path logDirectory = GbaConfig.getUserLogDirectory();
    final String dateString = Dates.format("yyyyMMdd_HHmmss", new Date(time));
    final File logFile = logDirectory.resolve("gba_tools_" + dateString + ".log").toFile();
    try {
      LogbackUtil.addRootFileAppender(logFile, "%d\t%p\t%c\t%m%n", false, "ca.bc.gov.gbasites", //
        "Starting application: " + " [" + GbaConfig.getEnvironment() + "] (v "
          + GbaSiteController.getVersion() + ")", //
        "User=" + GbaConfig.getUsername(), //
        "Host=" + InetAddress.getLocalHost() //
      );
    } catch (final IOException e) {
      Logs.error(GbaController.class, e);
    }
  }

  @Override
  protected void runDo() throws Throwable {
    super.runDo();
    final String title = "GBA Site Tools [" + GbaConfig.getEnvironment() + "] (v "
      + GbaSiteController.getVersion() + ")";
    this.frame = new JFrame(title);
    this.frame
      .setIconImages(Arrays.asList(Icons.getImage("gba_icon_32"), Icons.getImage("gba_icon_16")));

    this.frame.setLayout(new BorderLayout());
    this.frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

    newMenuBar();

    final JPanel sectionsPanel = new JPanel(new VerticalLayout(5));
    newPanelTasks(sectionsPanel);

    final TabbedPane bottomTabs = new TabbedPane();
    bottomTabs.setPreferredSize(new Dimension(400, 150));
    BackgroundTaskTableModel.addNewTabPanel(bottomTabs);
    LoggingTableModel.addNewTabPane(bottomTabs);

    final JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, sectionsPanel, bottomTabs);
    split.setResizeWeight(0.9);
    this.frame.add(split, BorderLayout.CENTER);

    this.frame.setMinimumSize(new Dimension(600, 300));
    this.frame.pack();
    SwingUtil.setLocationCentre(this.frame);
    Invoke.background("GBA Init", GbaItnDatabase::getRecordStore);
    this.frame.setVisible(true);
  }

  public void runProcess(final Class<?> klass, final BooleanEnableCheck enableCheck) {
    if (enableCheck.isEnabled()) {
      enableCheck.setEnabled(false);
      Invoke.background("Launching" + klass.getSimpleName(), () -> {
        final String logName = klass.getSimpleName() + "-" + Dates.format("yyy-MM-dd-hh-mm-ss");
        final JavaProcess javaProcess = GbaController.newJavaProcess(logName);
        javaProcess.setProgramClass(klass);
        javaProcess.setCompletedAction(() -> {
          enableCheck.setEnabled(true);
        });
        javaProcess.startThread();
      });
    }
  }
}
