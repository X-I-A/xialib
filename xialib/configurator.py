import abc


__all__ = ['Configurator']

class Configurator():  # pragma: no cover
    """Save all configurations
    """
    def __init__(self, insight_id: str, **kwargs):
        self.insight_id = insight_id

    @abc.abstractmethod
    def validate_insight(self) -> bool:
        """Public Function

        Attributes:
            insight_id (:obj:`str`): Insight ID to be validated
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_insight_config(self, config: dict):
        """Public Function

        Set insight.global level configurations
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_insight_config(self) -> dict:
        """Public Function

        Get insight configuration of section `global`, `library` and `services`,

        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_service(self, service_name: str, service_config: dict):
        """Public Function

        Set insight.services.service_name level configurations
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_library(self, library: dict):
        """Public Function

        Set insight.library information
        """
        raise NotImplementedError


    @abc.abstractmethod
    def set_data_route(self, topic_id: str, table_id: str, data_route: dict):
        """Public Function

        Set data route for each data source.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_data_route(self, topic_id: str, table_id: str) -> dict:
        raise NotImplementedError
