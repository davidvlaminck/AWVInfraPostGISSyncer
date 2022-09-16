class AgentMissingError(Exception):
    def __int__(self, agent_uuids: [str]):
        self.agent_uuids = agent_uuids
