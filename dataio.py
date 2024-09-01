import logging



def init_logging():
    logger = logging.getLogger()
    logger.setLevel('DEBUG')
    BASIC_FORMAT = '%(asctime)s - %(levelname)-5s - %(filename)-15s: line %(lineno)d - %(message)s'
    DATE_FORMATE = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMATE)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(filename = './result/dataio.log', mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


if __name__ == '__main__':
    init_logging()
    logging.info('=================data io begin to run===================')

    StorageManager().initialize()
    logging.info('create storage dir:' + StorageManager().get_current_storage_path())

    ServiceManager().initialize_all_services()
    ServiceManager().start_input_detector()

    while True:
        msg = PipeManager().input_detector_parent.recv()
        operation_type = ComponentMessageUtil.get_operation_type(msg)
        if operation_type = ComponentOperationType.START:
            logging.info('scheduler receive start signal from input detector.')
            ServiceManager().start_all_without_input_detector()
        elif operation_type = ComponentOperationType.STOP:
            logging.info('scheduler receive stop signal from input detector.')
            ServiceManager().stop_all_without_input_detector()
        elif operation_type = ComponentOperationType.UNKNOWN:
            logging.error('scheduler receive unknown signal from input detector.')
            ServiceManager().quit_all()
            break

    ServiceMnager.join_all()
    logging.info('data io complete to run...')